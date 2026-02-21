package com.dedicatedcode.paikka.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.NOT_FOUND)
public class POINotFoundException extends RuntimeException {
    
    public POINotFoundException(String message) {
        super(message);
    }
    
    public POINotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
