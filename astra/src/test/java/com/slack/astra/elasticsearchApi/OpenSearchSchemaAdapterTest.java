package com.slack.astra.elasticsearchApi;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class OpenSearchSchemaAdapterTest {
  @Test
  public void testRequestedFieldsStarWildcardMatchesFieldPatterns() {
    RequestedFields requestedFields = RequestedFields.from("message.*,service_*");

    assertThat(requestedFields.matches("message.keyword")).isTrue();
    assertThat(requestedFields.matches("service_name")).isTrue();
    assertThat(requestedFields.matches("message")).isFalse();
    assertThat(requestedFields.matches("service.name")).isFalse();
  }

  @Test
  public void testRequestedFieldsQuestionWildcardMatchesSingleCharacter() {
    RequestedFields requestedFields = RequestedFields.from("trace.?d,service_nam?");

    assertThat(requestedFields.matches("trace.id")).isTrue();
    assertThat(requestedFields.matches("trace.ids")).isFalse();
    assertThat(requestedFields.matches("service_name")).isTrue();
    assertThat(requestedFields.matches("service_names")).isFalse();
  }
}
