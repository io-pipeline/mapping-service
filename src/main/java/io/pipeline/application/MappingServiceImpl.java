package io.pipeline.application;

import com.google.protobuf.Value;
import io.pipeline.data.v1.*;
import io.pipeline.mapping.ApplyMappingRequest;
import io.pipeline.mapping.ApplyMappingResponse;
import io.pipeline.mapping.MappingRule;
import io.pipeline.mapping.MappingService;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Optional;

@GrpcService
public class MappingServiceImpl implements MappingService {

    private static final Logger LOG = Logger.getLogger(MappingServiceImpl.class);

    // Registration now handled by PlatformRegistrationClient
    // The old direct Consul registration has been removed

    @Override
    public Uni<ApplyMappingResponse> applyMapping(ApplyMappingRequest request) {
        PipeDoc.Builder docBuilder = request.getDocument().toBuilder();

        for (MappingRule rule : request.getRulesList()) {
            applyRule(docBuilder, rule);
        }

        return Uni.createFrom().item(ApplyMappingResponse.newBuilder()
                .setDocument(docBuilder.build())
                .build());
    }

    private void applyRule(PipeDoc.Builder docBuilder, MappingRule rule) {
        for (ProcessingMapping candidate : rule.getCandidateMappingsList()) {
            boolean success = false;
            switch (candidate.getMappingType()) {
                case MAPPING_TYPE_DIRECT:
                    success = handleDirectMapping(docBuilder, candidate);
                    break;
                case MAPPING_TYPE_TRANSFORM:
                    success = handleTransformMapping(docBuilder, candidate);
                    break;
                case MAPPING_TYPE_AGGREGATE:
                    success = handleAggregateMapping(docBuilder, candidate);
                    break;
                case MAPPING_TYPE_SPLIT:
                    success = handleSplitMapping(docBuilder, candidate);
                    break;
                case MAPPING_TYPE_UNSPECIFIED:
                default:
                    // Do nothing for unspecified or unknown types
                    break;
            }
            if (success) {
                return; // Move to the next rule once a candidate succeeds
            }
        }
    }

    private boolean handleDirectMapping(PipeDoc.Builder docBuilder, ProcessingMapping mapping) {
        if (mapping.getSourceFieldPathsCount() == 0 || mapping.getTargetFieldPathsCount() == 0) {
            return false; // Invalid DIRECT mapping
        }

        String sourcePath = mapping.getSourceFieldPaths(0);
        String targetPath = mapping.getTargetFieldPaths(0);

        Optional<Value> sourceValue = getFieldValue(docBuilder, sourcePath);

        if (sourceValue.isPresent()) {
            setFieldValue(docBuilder, targetPath, sourceValue.get());
            return true;
        }

        return false;
    }

    private boolean handleAggregateMapping(PipeDoc.Builder docBuilder, ProcessingMapping mapping) {
        if (mapping.getSourceFieldPathsCount() < 2 || mapping.getTargetFieldPathsCount() != 1) {
            return false; // Invalid AGGREGATE mapping
        }

        AggregateConfig config = mapping.getAggregateConfig();
        String targetPath = mapping.getTargetFieldPaths(0);

        switch (config.getAggregationType()) {
            case CONCATENATE:
                return handleConcatenate(docBuilder, mapping.getSourceFieldPathsList(), targetPath, config.getDelimiter());
            case SUM:
                return handleSum(docBuilder, mapping.getSourceFieldPathsList(), targetPath);
            default:
                return false;
        }
    }

    private boolean handleConcatenate(PipeDoc.Builder docBuilder, List<String> sourcePaths, String targetPath, String delimiter) {
        StringBuilder result = new StringBuilder();
        boolean first = true;
        for (String path : sourcePaths) {
            Optional<Value> value = getFieldValue(docBuilder, path);
            if (value.isEmpty() || value.get().getKindCase() != Value.KindCase.STRING_VALUE) {
                return false; // A source field was missing or not a string
            }
            if (!first) {
                result.append(delimiter);
            }
            result.append(value.get().getStringValue());
            first = false;
        }
        setFieldValue(docBuilder, targetPath, Value.newBuilder().setStringValue(result.toString()).build());
        return true;
    }

    private boolean handleSum(PipeDoc.Builder docBuilder, List<String> sourcePaths, String targetPath) {
        double sum = 0.0;
        for (String path : sourcePaths) {
            Optional<Value> value = getFieldValue(docBuilder, path);
            if (value.isEmpty() || value.get().getKindCase() != Value.KindCase.NUMBER_VALUE) {
                return false; // A source field was missing or not a number
            }
            sum += value.get().getNumberValue();
        }
        setFieldValue(docBuilder, targetPath, Value.newBuilder().setNumberValue(sum).build());
        return true;
    }

    private boolean handleSplitMapping(PipeDoc.Builder docBuilder, ProcessingMapping mapping) {
        if (mapping.getSourceFieldPathsCount() != 1 || mapping.getTargetFieldPathsCount() < 1) {
            return false; // Invalid SPLIT mapping
        }

        String sourcePath = mapping.getSourceFieldPaths(0);
        Optional<Value> sourceValue = getFieldValue(docBuilder, sourcePath);

        if (sourceValue.isEmpty() || sourceValue.get().getKindCase() != Value.KindCase.STRING_VALUE) {
            return false; // Source field must exist and be a string
        }

        String[] splitValues = sourceValue.get().getStringValue().split(mapping.getSplitConfig().getDelimiter());

        // Map the split values to the target fields, up to the number of targets specified
        for (int i = 0; i < mapping.getTargetFieldPathsCount(); i++) {
            if (i < splitValues.length) {
                String targetPath = mapping.getTargetFieldPaths(i);
                Value value = Value.newBuilder().setStringValue(splitValues[i]).build();
                setFieldValue(docBuilder, targetPath, value);
            }
        }

        return true;
    }

    private boolean handleTransformMapping(PipeDoc.Builder docBuilder, ProcessingMapping mapping) {
        if (mapping.getSourceFieldPathsCount() != 1 || mapping.getTargetFieldPathsCount() != 1) {
            return false; // Invalid TRANSFORM mapping
        }

        String sourcePath = mapping.getSourceFieldPaths(0);
        String targetPath = mapping.getTargetFieldPaths(0);
        TransformConfig config = mapping.getTransformConfig();

        Optional<Value> sourceValue = getFieldValue(docBuilder, sourcePath);
        if (sourceValue.isEmpty()) {
            return false;
        }

        switch (config.getRuleName().toLowerCase()) {
            case "uppercase":
                return transformUppercase(docBuilder, targetPath, sourceValue.get());
            case "trim":
                return transformTrim(docBuilder, targetPath, sourceValue.get());
            default:
                return false; // Unknown transformation rule
        }
    }

    private boolean transformUppercase(PipeDoc.Builder docBuilder, String targetPath, Value value) {
        if (value.getKindCase() != Value.KindCase.STRING_VALUE) {
            return false;
        }
        String upper = value.getStringValue().toUpperCase();
        setFieldValue(docBuilder, targetPath, Value.newBuilder().setStringValue(upper).build());
        return true;
    }

    private boolean transformTrim(PipeDoc.Builder docBuilder, String targetPath, Value value) {
        if (value.getKindCase() != Value.KindCase.STRING_VALUE) {
            return false;
        }
        String trimmed = value.getStringValue().trim();
        setFieldValue(docBuilder, targetPath, Value.newBuilder().setStringValue(trimmed).build());
        return true;
    }

    private Optional<Value> getFieldValue(PipeDoc.Builder docBuilder, String path) {
        // For now, we only support top-level custom fields.
        // This can be expanded to support nested paths (JSONPath-like).
        if (docBuilder.getSearchMetadata().getCustomFields().getFieldsMap().containsKey(path)) {
            return Optional.of(docBuilder.getSearchMetadata().getCustomFields().getFieldsMap().get(path));
        }
        return Optional.empty();
    }

    private void setFieldValue(PipeDoc.Builder docBuilder, String path, Value value) {
        // For now, we only support top-level custom fields.
        docBuilder.getSearchMetadataBuilder().getCustomFieldsBuilder().putFields(path, value);
    }
}