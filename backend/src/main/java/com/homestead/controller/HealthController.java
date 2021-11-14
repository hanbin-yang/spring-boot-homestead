package com.homestead.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author HanBin_Yang
 * @since 2021/11/13 18:17
 */
@RestController
@RequestMapping("/health")
public class HealthController {

    @GetMapping("/check")
    public String alive() {
        return "ok";
    }
}
