package com.slack.astra.elasticsearchApi.searchResponse;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.astra.testlib.MessageUtil;
import com.slack.astra.util.JsonUtil;
import java.util.List;
import org.junit.jupiter.api.Test;

public class SearchResponseHitTest {

  @Test
  void fromJsonStringUsesProvidedSortValuesWithoutReconstructingFromSource() throws Exception {
    SearchResponseHit hit =
        SearchResponseHit.fromJsonString(
            JsonUtil.writeAsString(MessageUtil.makeMessage(1)), List.of());

    assertThat(hit.getSort()).isEmpty();
  }

  @Test
  void fromJsonStringOmitsSortWhenNoSortValuesAreProvided() throws Exception {
    SearchResponseHit hit =
        SearchResponseHit.fromJsonString(JsonUtil.writeAsString(MessageUtil.makeMessage(1)), null);

    assertThat(JsonUtil.writeAsString(hit)).doesNotContain("\"sort\"");
  }
}
