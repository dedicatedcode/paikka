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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.intercept.AuthorizationFilter;

@Configuration
@EnableWebSecurity
public class SecurityConfig {
    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http, AdminTokenFilter adminTokenFilter) throws Exception {
        http
                .authorizeHttpRequests(authorize -> authorize
                        .requestMatchers("/", "/login", "/error", "/logout").permitAll()
                        .requestMatchers("/api/**", "/css/**", "/js/**", "/images/**", "/img/**", "/fonts/**").permitAll()
                        .anyRequest().authenticated()
                )
                .csrf(AbstractHttpConfigurer::disable)
                .addFilterBefore(adminTokenFilter, AuthorizationFilter.class)
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
                .authorities("ROLE_ADMIN")
                .password("{noop}"  + password)
                .build();
        return new InMemoryUserDetailsManager(user);
    }
}
