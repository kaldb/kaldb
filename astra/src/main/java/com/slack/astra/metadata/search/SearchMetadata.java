package com.slack.astra.metadata.search;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.astra.metadata.core.AstraMetadata;

/** Search metadata contains the metadata needed to perform a search on a snapshot. */
public class SearchMetadata extends AstraMetadata {
  public final String snapshotId;
  public final String snapshotName;
  public final String url;
  public final boolean liveChunkOnIndexer;
  private Boolean searchable;

  public SearchMetadata(
      String name,
      String snapshotName,
      String snapshotId,
      String url,
      Boolean searchable,
      boolean liveChunkOnIndexer) {
    super(name);
    checkArgument(searchable != null, "searchable cannot be null");
    checkArgument(url != null && !url.isEmpty(), "Url shouldn't be empty");
    checkArgument(
        snapshotName != null && !snapshotName.isEmpty(), "SnapshotName should not be empty");
    checkArgument(snapshotId != null && !snapshotId.isEmpty(), "SnapshotId should not be empty");
    this.snapshotId = snapshotId;
    this.snapshotName = snapshotName;
    this.url = url;
    this.searchable = searchable;
    this.liveChunkOnIndexer = liveChunkOnIndexer;
  }

  public static String generateSearchContextSnapshotId(String snapshotName, String hostname) {
    return snapshotName + "_" + hostname;
  }

  public Boolean isSearchable() {
    if (searchable == null) {
      return true;
    }
    return searchable;
  }

  public void setSearchable(Boolean searchable) {
    this.searchable = searchable;
  }

  public String getSnapshotName() {
    return snapshotName;
  }

  public String getSnapshotId() {
    return snapshotId;
  }

  public String getUrl() {
    return url;
  }

  public boolean isLiveChunkOnIndexer() {
    return liveChunkOnIndexer;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    SearchMetadata that = (SearchMetadata) o;

    if (!snapshotId.equals(that.snapshotId)) return false;
    if (!snapshotName.equals(that.snapshotName)) return false;
    if (liveChunkOnIndexer != that.liveChunkOnIndexer) return false;
    if (!searchable.equals(that.searchable)) return false;

    return url.equals(that.url);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + snapshotId.hashCode();
    result = 31 * result + snapshotName.hashCode();
    result = 31 * result + url.hashCode();
    result = 31 * result + Boolean.hashCode(liveChunkOnIndexer);
    result = 31 * result + searchable.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "SearchMetadata{"
        + "name='"
        + name
        + '\''
        + ", snapshotId='"
        + snapshotId
        + '\''
        + ", snapshotName='"
        + snapshotName
        + '\''
        + ", url='"
        + url
        + '\''
        + ", liveChunkOnIndexer="
        + liveChunkOnIndexer
        + ", searchable="
        + searchable
        + '}';
  }
}
