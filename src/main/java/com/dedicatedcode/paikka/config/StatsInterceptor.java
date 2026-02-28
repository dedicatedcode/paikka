/*
 *  This file is part of paikka.
 *
 *  Paikka is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU Affero General Public License
 *  as published by the Free Software Foundation, either version 3 or
 *  any later version.
 *
 *  Paikka is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied
 *  warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with Paikka. If not, see <https://www.gnu.org/licenses/>.
 */

package com.dedicatedcode.paikka.config;

import com.dedicatedcode.paikka.service.StatsService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Component
@ConditionalOnProperty(name = "paikka.import-mode", havingValue = "false", matchIfMissing = true)
public class StatsInterceptor implements HandlerInterceptor {
    
    private static final Logger logger = LoggerFactory.getLogger(StatsInterceptor.class);
    
    private final StatsService statsService;
    
    public StatsInterceptor(StatsService statsService) {
        this.statsService = statsService;
    }
    
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (request.getRequestURI().startsWith("/api/v1/")) {
            request.setAttribute("startTime", System.currentTimeMillis());
        }
        return true;
    }
    
    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, 
                               Object handler, Exception ex) {

        if (!request.getRequestURI().startsWith("/api/v1/") || 
            request.getAttribute("startTime") == null ||
            response.getStatus() >= 400) {
            return;
        }
        
        try {
            long responseTime = System.currentTimeMillis() - (Long) request.getAttribute("startTime");

            Map<String, String> sortedParams = request.getParameterMap().entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                    entry -> String.join(",", entry.getValue()),
                    (e1, e2) -> e1,
                    TreeMap::new
                ));

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
