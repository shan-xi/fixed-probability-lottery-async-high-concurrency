package com.example.lottery.api;

import jakarta.validation.constraints.Positive;

public record DrawRequest(@Positive long userId) { }
