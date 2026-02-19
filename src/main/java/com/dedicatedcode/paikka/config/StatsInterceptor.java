package com.dedicatedcode.paikka.config;

import com.dedicatedcode.paikka.service.StatsService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Component
public class StatsInterceptor implements HandlerInterceptor {
    
    private static final Logger logger = LoggerFactory.getLogger(StatsInterceptor.class);
    
    private final StatsService statsService;
    
    public StatsInterceptor(StatsService statsService) {
        this.statsService = statsService;
    }
    
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        // Only track API endpoints
        if (request.getRequestURI().startsWith("/api/v1/")) {
            request.setAttribute("startTime", System.currentTimeMillis());
        }
        return true;
    }
    
    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, 
                               Object handler, Exception ex) {
        
        // Only track API endpoints and successful requests
        if (!request.getRequestURI().startsWith("/api/v1/") || 
            request.getAttribute("startTime") == null ||
            response.getStatus() >= 400) {
            return;
        }
        
        try {
            long responseTime = System.currentTimeMillis() - (Long) request.getAttribute("startTime");
            
            // Extract and sort parameters
            Map<String, String> sortedParams = request.getParameterMap().entrySet().stream()
                .collect(Collectors.toMap(
                    entry -> entry.getKey(),
                    entry -> String.join(",", entry.getValue()),
                    (e1, e2) -> e1,
                    TreeMap::new
                ));
            
            // Extract result count from response header (set by controllers)
            int resultCount = 0;
            String resultCountHeader = response.getHeader("X-Result-Count");
            if (resultCountHeader != null) {
                try {
                    resultCount = Integer.parseInt(resultCountHeader);
                } catch (NumberFormatException e) {
                    logger.debug("Invalid result count header: {}", resultCountHeader);
                }
            }
            
            String clientIp = getClientIp(request);
            
            statsService.recordQuery(
                request.getRequestURI(),
                sortedParams,
                responseTime,
                resultCount,
                clientIp,
                response.getStatus()
            );
            
        } catch (Exception e) {
            logger.debug("Failed to record stats for request", e);
        }
    }
    
    private String getClientIp(HttpServletRequest request) {
        String xForwardedFor = request.getHeader("X-Forwarded-For");
        if (xForwardedFor != null && !xForwardedFor.isEmpty()) {
            return xForwardedFor.split(",")[0].trim();
        }
        
        String xRealIp = request.getHeader("X-Real-IP");
        if (xRealIp != null && !xRealIp.isEmpty()) {
            return xRealIp;
        }
        
        return request.getRemoteAddr();
    }
}
