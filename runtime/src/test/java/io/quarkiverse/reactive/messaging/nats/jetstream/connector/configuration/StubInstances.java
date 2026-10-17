package io.quarkiverse.reactive.messaging.nats.jetstream.connector.configuration;

import java.lang.annotation.Annotation;
import java.lang.reflect.Proxy;
import java.util.Map;

import jakarta.enterprise.inject.Instance;

import io.smallrye.common.annotation.Identifier;

/**
 * Proxy-backed {@link Instance} stubs that resolve beans by {@link Identifier}, without a CDI container.
 */
final class StubInstances {

    private StubInstances() {
    }

    @SuppressWarnings("unchecked")
    static <T> Instance<T> byIdentifier(final Map<String, T> beans) {
        return (Instance<T>) Proxy.newProxyInstance(Instance.class.getClassLoader(), new Class<?>[] { Instance.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("select") && args != null && args.length == 1
                            && args[0] instanceof Annotation[] qualifiers) {
                        for (Annotation qualifier : qualifiers) {
                            if (qualifier instanceof Identifier identifier) {
                                return single(beans.get(identifier.value()));
                            }
                        }
                        return single(null);
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    @SuppressWarnings("unchecked")
    private static <T> Instance<T> single(final T bean) {
        return (Instance<T>) Proxy.newProxyInstance(Instance.class.getClassLoader(), new Class<?>[] { Instance.class },
                (proxy, method, args) -> switch (method.getName()) {
                    case "isUnsatisfied" -> bean == null;
                    case "isResolvable" -> bean != null;
                    case "isAmbiguous" -> false;
                    case "select" -> single(null);
                    case "get" -> {
                        if (bean == null) {
                            throw new IllegalStateException("Unsatisfied dependency");
                        }
                        yield bean;
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
