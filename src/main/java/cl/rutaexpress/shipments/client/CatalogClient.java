package cl.rutaexpress.shipments.client;

import cl.rutaexpress.shipments.exception.CapacityExceededException;
import cl.rutaexpress.shipments.exception.UpstreamServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * Client for Catalog's internal capacity-decrement endpoint. This call is
 * synchronous because capacity is a hard business rule: a shipment must not be
 * accepted if the underlying catalog service has no remaining capacity. This
 * design decision (sync REST vs. async/eventual-consistency) is pending final
 * confirmation with Javier — see README "Design notes".
 */
@Component
public class CatalogClient {

    private static final Logger log = LoggerFactory.getLogger(CatalogClient.class);

    private final RestClient catalogRestClient;
    private final String internalApiKey;

    public CatalogClient(RestClient catalogRestClient,
                          @Value("${internal.api-key}") String internalApiKey) {
        this.catalogRestClient = catalogRestClient;
        this.internalApiKey = internalApiKey;
    }

    public record DecreaseCapacityResult(Long id, Integer capacity) {
    }

    private record DecreaseCapacityRequest(int amount) {
    }

    public void decreaseCapacity(Long serviceId) {
        try {
            catalogRestClient.post()
                    .uri("/api/catalog/services/{serviceId}/decrease-capacity", serviceId)
                    .header("X-Internal-Api-Key", internalApiKey)
                    .body(new DecreaseCapacityRequest(1))
                    .retrieve()
                    .onStatus(HttpStatusCode::is4xxClientError, (request, response) -> {
                        if (response.getStatusCode().value() == 409) {
                            throw new CapacityExceededException(
                                    "No capacity available for service " + serviceId);
                        }
                        if (response.getStatusCode().value() == 404) {
                            throw new CapacityExceededException(
                                    "Catalog service not found: " + serviceId);
                        }
                        throw new UpstreamServiceException(
                                "Catalog returned client error: " + response.getStatusCode());
                    })
                    .onStatus(HttpStatusCode::is5xxServerError, (request, response) -> {
                        throw new UpstreamServiceException(
                                "Catalog returned server error: " + response.getStatusCode());
                    })
                    .toBodilessEntity();
        } catch (CapacityExceededException | UpstreamServiceException e) {
            throw e;
        } catch (RestClientException e) {
            log.error("Failed to call Catalog service for serviceId={}", serviceId, e);
            throw new UpstreamServiceException("Catalog service unreachable", e);
        }
    }
}
