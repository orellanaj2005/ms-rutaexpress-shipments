package cl.rutaexpress.shipments.security;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;

import java.util.List;

/**
 * Usuario que hace la operación, tomado del JWT de Azure AD: nombre desde {@code preferred_username}
 * (o {@code name}, o {@code sub}) y rol desde el primer valor de {@code roles}.
 */
public record CurrentUser(String name, String role) {

    public static final CurrentUser SYSTEM = new CurrentUser("sistema", "Sistema");

    public static CurrentUser fromSecurityContext() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth == null || !(auth.getPrincipal() instanceof Jwt jwt)) {
            return SYSTEM;
        }
        String name = firstNonBlank(jwt.getClaimAsString("preferred_username"),
                jwt.getClaimAsString("name"), jwt.getSubject());
        List<String> roles = jwt.getClaimAsStringList("roles");
        String role = roles == null || roles.isEmpty() ? "SinRol" : roles.getFirst();
        return new CurrentUser(name == null ? SYSTEM.name() : name, role);
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }
}
