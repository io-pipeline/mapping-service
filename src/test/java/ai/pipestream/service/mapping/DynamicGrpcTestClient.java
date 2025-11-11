package ai.pipestream.service.mapping;

import io.pipeline.dynamic.grpc.client.DynamicGrpcClientFactory;
import io.pipeline.platform.registration.*;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Test client to verify dynamic gRPC discovery works
 */
@ApplicationScoped
public class DynamicGrpcTestClient {
    
    private static final Logger LOG = Logger.getLogger(DynamicGrpcTestClient.class);
    
    @Inject
    DynamicGrpcClientFactory grpcClientFactory;
    
    void onStart(@Observes StartupEvent ev) {
        LOG.info("Testing dynamic gRPC client discovery...");
        
        // Try to dynamically discover and connect to platform-registration-service
        testDynamicDiscovery();
    }
    
    private void testDynamicDiscovery() {
        String serviceName = "platform-registration-service";
        
        LOG.infof("Attempting to dynamically discover service: %s", serviceName);
        
        // This should dynamically:
        // 1. Look up the service in Consul via Stork
        // 2. Create a gRPC channel
        // 3. Return a stub
        grpcClientFactory.getPlatformRegistrationClient(serviceName)
            .onItem().transformToUni(stub -> {
                LOG.infof("Successfully got stub for %s, testing connection...", serviceName);
                
                // Test the connection by listing services
                return stub.listServices(com.google.protobuf.Empty.getDefaultInstance())
                    .map(response -> {
                        LOG.infof("Dynamic gRPC SUCCESS! Found %d services registered", response.getTotalCount());
                        response.getServicesList().forEach(service -> 
                            LOG.infof("  - %s at %s:%d", service.getServiceName(), service.getHost(), service.getPort())
                        );
                        return true;
                    });
            })
            .subscribe().with(
                success -> LOG.info("Dynamic gRPC client test completed successfully!"),
                failure -> {
                    LOG.errorf(failure, "Dynamic gRPC client test FAILED");
                    
                    // Log more details about the failure
                    if (failure instanceof io.grpc.StatusRuntimeException) {
                        io.grpc.StatusRuntimeException sre = (io.grpc.StatusRuntimeException) failure;
                        LOG.errorf("gRPC Status: %s", sre.getStatus());
                        LOG.errorf("Description: %s", sre.getStatus().getDescription());
                    }
                    
                    // Let's also try to debug what Stork sees
                    debugStorkServices();
                }
            );
    }
    
    private void debugStorkServices() {
        try {
            LOG.info("Debugging Stork service discovery...");
            
            // Try to get service info from Stork directly
            var stork = io.smallrye.stork.Stork.getInstance();
            var serviceOptional = stork.getServiceOptional("platform-registration-service");
            
            if (serviceOptional.isPresent()) {
                LOG.info("Service found in Stork!");
                var service = serviceOptional.get();
                
                service.getInstances()
                    .subscribe().with(
                        instances -> {
                            LOG.infof("Found %d instances:", instances.size());
                            instances.forEach(instance -> 
                                LOG.infof("  - Instance at %s:%d", instance.getHost(), instance.getPort())
                            );
                        },
                        error -> LOG.errorf(error, "Failed to get instances from Stork")
                    );
            } else {
                LOG.warn("Service NOT found in Stork - this is the problem!");
                
                // Try to manually check what's happening
                LOG.info("Service discovery issue - service not found in Stork");
                LOG.info("This means Stork hasn't been configured for this service yet");
                LOG.info("The DynamicGrpcClientFactory should have defined it automatically");
            }
            
        } catch (Exception e) {
            LOG.error("Error debugging Stork", e);
        }
    }
}