package df.digitalfix_ms_admin_queues.security;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class UserRoleClient {

    private static final Duration ROLE_TTL = Duration.ofMinutes(5);
    private static final Duration MISSING_ROLE_TTL = Duration.ofMinutes(1);
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration READ_TIMEOUT = Duration.ofSeconds(8);

    private record CachedRole(Optional<String> role, Instant expiresAt) {
    }

    private final RestClient restClient;
    private final Map<String, CachedRole> cache = new ConcurrentHashMap<>();

    public UserRoleClient(@Value("${digitalfix.ms-user.url:http://localhost:8082}") String msUserUrl) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(CONNECT_TIMEOUT);
        requestFactory.setReadTimeout(READ_TIMEOUT);
        this.restClient = RestClient.builder()
                .baseUrl(msUserUrl)
                .requestFactory(requestFactory)
                .build();
    }

    public Optional<String> findRoleCode(String userEntraId, String bearerToken) {
        if (userEntraId == null || userEntraId.isBlank()) {
            return Optional.empty();
        }
        CachedRole cached = cache.get(userEntraId);
        Instant now = Instant.now();
        if (cached != null && now.isBefore(cached.expiresAt())) {
            return cached.role();
        }
        Optional<String> fetched = fetchRole(userEntraId, bearerToken);
        cache.put(userEntraId, new CachedRole(fetched, now.plus(fetched.isPresent() ? ROLE_TTL : MISSING_ROLE_TTL)));
        return fetched;
    }

    @SuppressWarnings("unchecked")
    private Optional<String> fetchRole(String userEntraId, String bearerToken) {
        try {
            Map<String, Object> profile = restClient.get()
                    .uri("/api/user/me")
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken)
                    .retrieve()
                    .body(Map.class);
            if (profile == null) {
                return Optional.empty();
            }
            Object role = profile.get("rolCodigo");
            if (role instanceof String code && !code.isBlank()) {
                return Optional.of(code);
            }
            return Optional.empty();
        } catch (RestClientException ex) {
            log.debug("No se pudo obtener el rol desde ms-user para {}: {}", userEntraId, ex.getMessage());
            return Optional.empty();
        }
    }
}
