package df.digitalfix_ms_admin_queues.security;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.access.hierarchicalroles.RoleHierarchy;
import org.springframework.security.access.hierarchicalroles.RoleHierarchyImpl;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.*;
import org.springframework.security.web.SecurityFilterChain;

import java.util.List;
import java.util.Set;

@Configuration
@EnableWebSecurity
@RequiredArgsConstructor
public class SecurityConfig {

    private final EntraJwtAuthenticationConverter jwtAuthenticationConverter;
    private final JsonSecurityErrorHandler jsonSecurityErrorHandler;

    @Value("${AZURE_TENANT_ID}")
    private String tenantId;

    @Value("${AZURE_CLIENT_ID}")
    private String clientId;

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        return http
                .csrf(AbstractHttpConfigurer::disable)
                .cors(Customizer.withDefaults())
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers("/actuator/health", "/actuator/health/**").permitAll()
                        .requestMatchers("/v3/api-docs/**", "/swagger-ui/**", "/swagger-ui.html").permitAll()
                        .requestMatchers("/internal/**").permitAll()
                        .requestMatchers(HttpMethod.OPTIONS, "/**").permitAll()
                        .requestMatchers("/api/v1/**").hasAnyRole("ADMIN", "SUPERADMIN")
                        .anyRequest().authenticated()
                )
                .oauth2ResourceServer(oauth2 -> oauth2
                        .jwt(jwt -> jwt
                                .decoder(jwtDecoder())
                                .jwtAuthenticationConverter(jwtAuthenticationConverter))
                        .authenticationEntryPoint(jsonSecurityErrorHandler)
                        .accessDeniedHandler(jsonSecurityErrorHandler))
                .exceptionHandling(ex -> ex
                        .authenticationEntryPoint(jsonSecurityErrorHandler)
                        .accessDeniedHandler(jsonSecurityErrorHandler))
                .build();
    }

    @Bean
    static RoleHierarchy roleHierarchy() {
        return RoleHierarchyImpl.withDefaultRolePrefix()
                .role("SUPERADMIN").implies("ADMIN", "SUPERVISOR", "TECNICO", "CLIENTE", "AUDITOR")
                .build();
    }

    @Bean
    public JwtDecoder jwtDecoder() {
        String issuerV2 = "https://login.microsoftonline.com/" + tenantId + "/v2.0";
        String issuerV1 = "https://sts.windows.net/" + tenantId + "/";

        NimbusJwtDecoder jwtDecoder = JwtDecoders.fromIssuerLocation(issuerV2);

        Set<String> allowedIssuers = Set.of(issuerV2, issuerV1);
        OAuth2TokenValidator<Jwt> issuerValidator = new JwtClaimValidator<Object>(
                JwtClaimNames.ISS, iss -> iss != null && allowedIssuers.contains(iss.toString()));

        OAuth2TokenValidator<Jwt> audienceValidator = new JwtClaimValidator<Object>(
                JwtClaimNames.AUD, aud -> {
                    if (aud instanceof String s) {
                        return s.equals(clientId) || s.equals("api://" + clientId);
                    }
                    if (aud instanceof List<?> list) {
                        return list.contains(clientId) || list.contains("api://" + clientId);
                    }
                    return false;
                });

        jwtDecoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(
                JwtValidators.createDefault(), issuerValidator, audienceValidator));
        return jwtDecoder;
    }
}
