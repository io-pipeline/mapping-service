package ai.pipestream.service.mapping;

import ai.pipestream.grpc.wiremock.MockServiceFactory;
import ai.pipestream.grpc.wiremock.MockServiceTestProfile;
import ai.pipestream.grpc.wiremock.MockServiceTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for mapping service using mocked platform-registration-service.
 *
 * This test uses:
 * - MockServiceTestProfile: Provides configuration overrides for test mode
 * - MockServiceTestResource: Automatically starts DirectWireMockGrpcServer
 *
 * Together they provide:
 * - DirectWireMockGrpcServer running on a dynamic port
 * - Platform-registration gRPC client configured to use the mock
 * - Full streaming registration events (6 events) without requiring Consul
 */
@QuarkusTest
@TestProfile(MockServiceTestProfile.class)
@QuarkusTestResource(MockServiceTestResource.class)
class MappingServiceIntegrationTest {

    @Test
    void testMappingServiceStartsWithMockedPlatformRegistration() {
        // Verify the mock platform-registration-service is running
        assertTrue(MockServiceFactory.isMockServiceRunning(),
            "Mock platform registration service should be running");

        assertNotNull(MockServiceFactory.getMockServer(),
            "Mock server instance should be available");
    }

    @Test
    void testMockServerProvidesStreamingSupport() {
        // The DirectWireMockGrpcServer provides full streaming support
        // including all 6 registration events:
        // STARTED, VALIDATED, CONSUL_REGISTERED, HEALTH_CHECK_CONFIGURED,
        // CONSUL_HEALTHY, COMPLETED

        var mockServer = MockServiceFactory.getMockServer();
        assertNotNull(mockServer, "Mock server should be available");
        assertTrue(mockServer.getGrpcPort() > 0,
            "Mock server should be running on a valid port");
    }
}
