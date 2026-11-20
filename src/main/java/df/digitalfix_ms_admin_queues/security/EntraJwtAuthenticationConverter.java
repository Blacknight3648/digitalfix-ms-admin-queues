package df.digitalfix_ms_admin_queues.security;

import lombok.RequiredArgsConstructor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Component
@RequiredArgsConstructor
public class EntraJwtAuthenticationConverter implements Converter<Jwt, AbstractAuthenticationToken> {

    private final UserRoleClient userRoleClient;

    @Override
    public AbstractAuthenticationToken convert(Jwt jwt) {
        Set<GrantedAuthority> authorities = new LinkedHashSet<>();

        String scopes = jwt.getClaimAsString("scp");
        if (scopes != null) {
            for (String scope : scopes.split(" ")) {
                if (!scope.isBlank()) {
                    authorities.add(new SimpleGrantedAuthority("SCOPE_" + scope));
                }
            }
        }

        List<String> tokenRoles = jwt.getClaimAsStringList("roles");
        if (tokenRoles != null) {
            tokenRoles.forEach(role -> authorities.add(toRoleAuthority(role)));
        }

        String userEntraId = EntraTokenClaims.userEntraId(jwt);
        userRoleClient.findRoleCode(userEntraId, jwt.getTokenValue())
                .ifPresent(role -> authorities.add(toRoleAuthority(role)));

        return new JwtAuthenticationToken(jwt, authorities, userEntraId);
    }

    private static GrantedAuthority toRoleAuthority(String role) {
        return new SimpleGrantedAuthority("ROLE_" + role.toUpperCase());
    }
}
