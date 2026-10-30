package com.example.lottery.api;

import com.example.lottery.domain.PrizeType;
import com.example.lottery.service.AsyncDrawService;
import com.example.lottery.service.PrizeInventoryService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/lottery")
public class DrawController {
    private final AsyncDrawService drawService;
    private final PrizeInventoryService inventoryService;

    public DrawController(AsyncDrawService drawService, PrizeInventoryService inventoryService) {
        this.drawService = drawService;
        this.inventoryService = inventoryService;
    }

    /** Persist a PENDING draw + outbox event, then return immediately with 202 Accepted. */
    @PostMapping("/draws")
    public ResponseEntity<AsyncDrawResponse> submit(@Valid @RequestBody DrawRequest request) {
        AsyncDrawService.Submission submission = drawService.submit(request.userId());
        AsyncDrawResponse response = submission.response();
        if (submission.alreadySubmitted()) {
            return ResponseEntity.ok(response);
        }
        return ResponseEntity.accepted().location(URI.create("/api/lottery/draws/id/" + response.drawId()))
                .body(response);
    }

    /** Lookup by user ID; returns PENDING until the Kafka worker finishes. */
    @GetMapping("/draws/{userId}")
    public ResponseEntity<AsyncDrawResponse> getByUser(@PathVariable long userId) {
        AsyncDrawResponse result = drawService.findByUserId(userId);
        return result == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(result);
    }

    @GetMapping("/draws/id/{drawId}")
    public ResponseEntity<AsyncDrawResponse> getByDrawId(@PathVariable long drawId) {
        AsyncDrawResponse result = drawService.findByDrawId(drawId);
        return result == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(result);
    }

    @GetMapping("/prizes")
    public List<Map<String, Object>> prizes() {
        return Arrays.stream(PrizeType.values()).filter(PrizeType::isWeightedPrize).map(p -> Map.<String, Object>of(
                "prizeType", p.name(), "probabilityPercent", p.probabilityPercent(),
                "probabilityBasisPoints", p.probabilityBasisPoints(), "limited", p.isLimited())).toList();
    }

    @GetMapping("/inventory")
    public List<PrizeInventoryService.InventoryView> inventory() { return inventoryService.list(); }
}
