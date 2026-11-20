package df.digitalfix_ms_admin_queues.security;

import org.springframework.security.oauth2.jwt.Jwt;

public final class EntraTokenClaims {

    private EntraTokenClaims() {
    }

    public static String userEntraId(Jwt jwt) {
        String oid = jwt.getClaimAsString("oid");
        return oid != null ? oid : jwt.getSubject();
    }
}
