package ai.pipestream.service.mapping;

import com.google.protobuf.util.Values;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.ProcessingMapping;
import ai.pipestream.mapping.ApplyMappingRequest;
import ai.pipestream.mapping.ApplyMappingResponse;
import ai.pipestream.mapping.MappingRule;
import org.junit.jupiter.api.Test;

import com.google.protobuf.Struct;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.MappingType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MappingServiceImplTest {

    @Test
    void testApplyMappingWithFallback() {
        MappingServiceImpl service = new MappingServiceImpl();

        // 1. Create a source PipeDoc with custom fields in SearchMetadata
        Struct customFields = Struct.newBuilder()
                .putFields("headline", Values.of("This is the headline"))
                .putFields("some_other_field", Values.of("some value"))
                .build();

        SearchMetadata searchMetadata = SearchMetadata.newBuilder()
                .setCustomFields(customFields)
                .build();

        PipeDoc sourceDoc = PipeDoc.newBuilder()
                .setSearchMetadata(searchMetadata)
                .build();

        // 2. Define the mapping rule with a fallback
        ProcessingMapping primaryMapping = ProcessingMapping.newBuilder()
                .setMappingType(MappingType.MAPPING_TYPE_DIRECT)
                .addSourceFieldPaths("title") // This field doesn't exist
                .addTargetFieldPaths("output_title")
                .build();

        ProcessingMapping fallbackMapping = ProcessingMapping.newBuilder()
                .setMappingType(MappingType.MAPPING_TYPE_DIRECT)
                .addSourceFieldPaths("headline") // This one exists
                .addTargetFieldPaths("output_title")
                .build();

        MappingRule titleRule = MappingRule.newBuilder()
                .addCandidateMappings(primaryMapping)
                .addCandidateMappings(fallbackMapping)
                .build();

        // 3. Build the request
        ApplyMappingRequest request = ApplyMappingRequest.newBuilder()
                .setDocument(sourceDoc)
                .addRules(titleRule)
                .build();

        // 4. Call the service
        ApplyMappingResponse response = service.applyMapping(request).await().indefinitely();

        // 5. Verify the result
        PipeDoc resultDoc = response.getDocument();
        assertTrue(resultDoc.getSearchMetadata().getCustomFields().getFieldsMap().containsKey("output_title"), "The target field should exist.");
        assertEquals("This is the headline", resultDoc.getSearchMetadata().getCustomFields().getFieldsOrThrow("output_title").getStringValue());
    }

    @Test
    void testConcatenateMapping() {
        MappingServiceImpl service = new MappingServiceImpl();

        Struct customFields = Struct.newBuilder()
                .putFields("first_name", Values.of("John"))
                .putFields("last_name", Values.of("Doe"))
                .build();

        PipeDoc sourceDoc = PipeDoc.newBuilder()
                .setSearchMetadata(SearchMetadata.newBuilder().setCustomFields(customFields))
                .build();

        ProcessingMapping concatMapping = ProcessingMapping.newBuilder()
                .setMappingType(MappingType.MAPPING_TYPE_AGGREGATE)
                .addSourceFieldPaths("first_name")
                .addSourceFieldPaths("last_name")
                .addTargetFieldPaths("full_name")
                .setAggregateConfig(ai.pipestream.data.v1.AggregateConfig.newBuilder()
                        .setAggregationType(ai.pipestream.data.v1.AggregateConfig.AggregationType.CONCATENATE)
                        .setDelimiter(" "))
                .build();

        ApplyMappingRequest request = ApplyMappingRequest.newBuilder()
                .setDocument(sourceDoc)
                .addRules(MappingRule.newBuilder().addCandidateMappings(concatMapping))
                .build();

        ApplyMappingResponse response = service.applyMapping(request).await().indefinitely();

        PipeDoc resultDoc = response.getDocument();
        assertTrue(resultDoc.getSearchMetadata().getCustomFields().getFieldsMap().containsKey("full_name"));
        assertEquals("John Doe", resultDoc.getSearchMetadata().getCustomFields().getFieldsOrThrow("full_name").getStringValue());
    }

    @Test
    void testSumMapping() {
        MappingServiceImpl service = new MappingServiceImpl();

        Struct customFields = Struct.newBuilder()
                .putFields("val1", Values.of(10.5))
                .putFields("val2", Values.of(20.5))
                .build();

        PipeDoc sourceDoc = PipeDoc.newBuilder()
                .setSearchMetadata(SearchMetadata.newBuilder().setCustomFields(customFields))
                .build();

        ProcessingMapping sumMapping = ProcessingMapping.newBuilder()
                .setMappingType(MappingType.MAPPING_TYPE_AGGREGATE)
                .addSourceFieldPaths("val1")
                .addSourceFieldPaths("val2")
                .addTargetFieldPaths("sum")
                .setAggregateConfig(ai.pipestream.data.v1.AggregateConfig.newBuilder()
                        .setAggregationType(ai.pipestream.data.v1.AggregateConfig.AggregationType.SUM))
                .build();

        ApplyMappingRequest request = ApplyMappingRequest.newBuilder()
                .setDocument(sourceDoc)
                .addRules(MappingRule.newBuilder().addCandidateMappings(sumMapping))
                .build();

        ApplyMappingResponse response = service.applyMapping(request).await().indefinitely();

        PipeDoc resultDoc = response.getDocument();
        assertTrue(resultDoc.getSearchMetadata().getCustomFields().getFieldsMap().containsKey("sum"));
        assertEquals(31.0, resultDoc.getSearchMetadata().getCustomFields().getFieldsOrThrow("sum").getNumberValue(), 0.001);
    }

    @Test
    void testSplitMapping() {
        MappingServiceImpl service = new MappingServiceImpl();

        Struct customFields = Struct.newBuilder()
                .putFields("full_name", Values.of("John Doe"))
                .build();

        PipeDoc sourceDoc = PipeDoc.newBuilder()
                .setSearchMetadata(SearchMetadata.newBuilder().setCustomFields(customFields))
                .build();

        ProcessingMapping splitMapping = ProcessingMapping.newBuilder()
                .setMappingType(MappingType.MAPPING_TYPE_SPLIT)
                .addSourceFieldPaths("full_name")
                .addTargetFieldPaths("first_name")
                .addTargetFieldPaths("last_name")
                .setSplitConfig(ai.pipestream.data.v1.SplitConfig.newBuilder().setDelimiter(" "))
                .build();

        ApplyMappingRequest request = ApplyMappingRequest.newBuilder()
                .setDocument(sourceDoc)
                .addRules(MappingRule.newBuilder().addCandidateMappings(splitMapping))
                .build();

        ApplyMappingResponse response = service.applyMapping(request).await().indefinitely();

        PipeDoc resultDoc = response.getDocument();
        assertTrue(resultDoc.getSearchMetadata().getCustomFields().getFieldsMap().containsKey("first_name"));
        assertTrue(resultDoc.getSearchMetadata().getCustomFields().getFieldsMap().containsKey("last_name"));
        assertEquals("John", resultDoc.getSearchMetadata().getCustomFields().getFieldsOrThrow("first_name").getStringValue());
        assertEquals("Doe", resultDoc.getSearchMetadata().getCustomFields().getFieldsOrThrow("last_name").getStringValue());
    }

    @Test
    void testTransformUppercase() {
        MappingServiceImpl service = new MappingServiceImpl();

        Struct customFields = Struct.newBuilder()
                .putFields("lower", Values.of("hello world"))
                .build();

        PipeDoc sourceDoc = PipeDoc.newBuilder()
                .setSearchMetadata(SearchMetadata.newBuilder().setCustomFields(customFields))
                .build();

        ProcessingMapping transformMapping = ProcessingMapping.newBuilder()
                .setMappingType(MappingType.MAPPING_TYPE_TRANSFORM)
                .addSourceFieldPaths("lower")
                .addTargetFieldPaths("upper")
                .setTransformConfig(ai.pipestream.data.v1.TransformConfig.newBuilder().setRuleName("uppercase"))
                .build();

        ApplyMappingRequest request = ApplyMappingRequest.newBuilder()
                .setDocument(sourceDoc)
                .addRules(MappingRule.newBuilder().addCandidateMappings(transformMapping))
                .build();

        ApplyMappingResponse response = service.applyMapping(request).await().indefinitely();

        PipeDoc resultDoc = response.getDocument();
        assertTrue(resultDoc.getSearchMetadata().getCustomFields().getFieldsMap().containsKey("upper"));
        assertEquals("HELLO WORLD", resultDoc.getSearchMetadata().getCustomFields().getFieldsOrThrow("upper").getStringValue());
    }

    @Test
    void testTransformTrim() {
        MappingServiceImpl service = new MappingServiceImpl();

        Struct customFields = Struct.newBuilder()
                .putFields("untrimmed", Values.of("  hello world  "))
                .build();

        PipeDoc sourceDoc = PipeDoc.newBuilder()
                .setSearchMetadata(SearchMetadata.newBuilder().setCustomFields(customFields))
                .build();

        ProcessingMapping transformMapping = ProcessingMapping.newBuilder()
                .setMappingType(MappingType.MAPPING_TYPE_TRANSFORM)
                .addSourceFieldPaths("untrimmed")
                .addTargetFieldPaths("trimmed")
                .setTransformConfig(ai.pipestream.data.v1.TransformConfig.newBuilder().setRuleName("trim"))
                .build();

        ApplyMappingRequest request = ApplyMappingRequest.newBuilder()
                .setDocument(sourceDoc)
                .addRules(MappingRule.newBuilder().addCandidateMappings(transformMapping))
                .build();

        ApplyMappingResponse response = service.applyMapping(request).await().indefinitely();

        PipeDoc resultDoc = response.getDocument();
        assertTrue(resultDoc.getSearchMetadata().getCustomFields().getFieldsMap().containsKey("trimmed"));
        assertEquals("hello world", resultDoc.getSearchMetadata().getCustomFields().getFieldsOrThrow("trimmed").getStringValue());
    }
}