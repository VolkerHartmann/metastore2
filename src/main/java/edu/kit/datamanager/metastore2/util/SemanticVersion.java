package edu.kit.datamanager.metastore2.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Objects;

public class SemanticVersion implements Comparable<SemanticVersion> {
  /**
   * Logger for messages.
   */
  private static final Logger LOG = LoggerFactory.getLogger(SemanticVersion.class);
  /**
   * Major version number.
   */
  private int major = 0;
  /**
   * Minor version number.
   */
  private int minor = 0;
  /**
   * Patch version number.
   */
  private int patch = 0;
  /**
   * Suffix for pre-release or build metadata.
   */
  private String suffix = null; // e.g. "-RC1", "-M9", "-SNAPSHOT"

  public SemanticVersion() {
    incrementPatch();
  }
  /**
   * Constructor from version string.
   * If there are missing levels they will be set to '0'
   *
   * @param version Version string in format MAJOR.MINOR.PATCH[-SUFFIX]
   */
  public SemanticVersion(String version) {
    if (version != null) {
      String[] mainAndSuffix = version.split("-", 2);
      String[] parts = mainAndSuffix[0].split("\\.");
      switch (parts.length) {
        case 3:
          patch = Integer.parseInt(parts[2]);
        case 2:
          minor = Integer.parseInt(parts[1]);
        case 1:
          major = Integer.parseInt(parts[0]);
      }
      if (parts.length != 3) {
        LOG.trace("Version has to be in format MAJOR.MINOR.PATCH[-SUFFIX] but found '{}'. Transformed to '{}'.", version, toString());
      }

      this.suffix = mainAndSuffix.length > 1 ? mainAndSuffix[1] : null;
    }
  }

  public int getMajor() {
    return major;
  }

  public int getMinor() {
    return minor;
  }

  public int getPatch() {
    return patch;
  }

  public String getSuffix() {
    return suffix;
  }

  public SemanticVersion incrementPatch() {
    patch++;
    suffix = null;
    return this;
  }

  public SemanticVersion incrementMinor() {
    minor++;
    patch = 0;
    suffix = null;
    return this;
  }

  public SemanticVersion incrementMajor() {
    major++;
    minor = 0;
    patch = 0;
    suffix = null;
    return this;
  }

  /**
   * Transform string to semantic version string (if possible)
   * e.g.: 3.2 -> 3.2.0, 1-RC1 -> 1.0.0-RC1
   * @param version
   * @return semantic version.
   */
  public static String parseVersion(String version) {
    String semanticVersion = version;
    if (version != null) {
      semanticVersion = new SemanticVersion(version).toString();
    }
    return semanticVersion;
  }

  @Override
  public int compareTo(SemanticVersion other) {
    if (this.major != other.major) {
      return Integer.compare(this.major, other.major);
    }
    if (this.minor != other.minor) {
      return Integer.compare(this.minor, other.minor);
    }
    return Integer.compare(this.patch, other.patch);
    // Suffix will be ignored for comparison
  }

  @Override
  public String toString() {
    return major + "." + minor + "." + patch + (suffix != null ? "-" + suffix : "");
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof SemanticVersion)) return false;
    SemanticVersion other = (SemanticVersion) obj;
    return this.major == other.major &&
            this.minor == other.minor &&
            this.patch == other.patch &&
            Objects.equals(this.suffix, other.suffix);
  }

  @Override
  public int hashCode() {
    return Objects.hash(major, minor, patch, suffix);
  }
}
