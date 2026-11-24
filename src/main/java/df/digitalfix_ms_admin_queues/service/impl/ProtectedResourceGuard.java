package df.digitalfix_ms_admin_queues.service.impl;

import df.digitalfix_ms_admin_queues.exception.ProtectedResourceException;

import java.util.List;

/**
 * Validación compartida contra {@code app.admin.protected-resources} (nombres exactos o prefijos con {@code *}).
 */
final class ProtectedResourceGuard {

    private ProtectedResourceGuard() {
    }

    static void ensureNotProtected(String resourceName, List<String> protectedResources, String resourceLabel) {
        if (protectedResources == null || protectedResources.isEmpty()) {
            return;
        }
        boolean isProtected = protectedResources.stream()
                .map(String::trim)
                .filter(pattern -> !pattern.isEmpty())
                .anyMatch(pattern -> pattern.endsWith("*")
                        ? resourceName.startsWith(pattern.substring(0, pattern.length() - 1))
                        : resourceName.equals(pattern));

        if (isProtected) {
            throw new ProtectedResourceException(
                    "'" + resourceName + "' es un recurso protegido del sistema (" + resourceLabel
                            + ") y no se puede eliminar ni desvincular desde la API");
        }
    }
}
