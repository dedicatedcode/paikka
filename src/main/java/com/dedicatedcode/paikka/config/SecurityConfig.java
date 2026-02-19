package com.dedicatedcode.paikka.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableWebSecurity
public class SecurityConfig {
    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
                .authorizeHttpRequests(authorize -> authorize
                        .requestMatchers("/", "/login", "/error", "/logout").permitAll()
                        .requestMatchers("/api/**", "/css/**", "/js/**", "/images/**", "/img/**", "/fonts/**").permitAll()
                        .anyRequest().authenticated()
                )
                .formLogin(form -> form
                        .loginPage("/login")
                )
                .rememberMe(rememberMe -> rememberMe
                        .key("uniqueAndSecretKey")
                        .tokenValiditySeconds(2592000) // 30 days
                        .rememberMeParameter("remember-me")
                        .useSecureCookie(false)
                )
                .logout(logout -> {
                    logout.deleteCookies("JSESSIONID", "remember-me")
                            .permitAll();
                });

        return http.build();
    }

    @Bean
    public UserDetailsService userDetailsService(@Value("${paikka.admin.password}") String password) {
        UserDetails user = User.builder()
                .username("admin")
                .password("{noop}"  + password)
                .build();
        return new InMemoryUserDetailsManager(user);
    }
}
