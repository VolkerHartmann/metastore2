package edu.kit.datamanager.metastore2.util;
import jakarta.validation.constraints.NotNull;

import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a semantic version in the format MAJOR.MINOR.PATCH
 * (without pre-release or build metadata).
 * Valid examples: 0.0.0, 1.2.3, 10.11.12
 * Invalid examples: 01.2.3, 1.2, 1.2.3-alpha, 1.2.3+build, 1.2.3-SNAPSHOT
 */
public final class SemanticVersion implements Comparable<SemanticVersion> {

  public enum INCREMENT_LEVEL { MAJOR, MINOR, PATCH }

  /**
   * Regex pattern for semantic versioning (MAJOR.MINOR.PATCH).
   * ^  -> Start of the String
   * (0|[1-9]\d*) -> Major version: either 0 or a non-zero digit followed by digits
   * \. -> Literal dot
   * (0|[1-9]\d*) -> Minor version: same pattern as major
   * \. -> Literal dot
   * (0|[1-9]\d*) -> Patch version: same pattern as major
   * $  -> End of the String
   */
  public static final Pattern VERSION_PATTERN = Pattern.compile(
          "^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)$"
  );
  /** Major version number. */
  private final int major;
  /** Minor version number. */
  private final int minor;
  /** Patch version number. */
  private final int patch;

  private SemanticVersion(int major, int minor, int patch) {
    // all components are non-negative due to regex and parseInt
    if (major < 0 || minor < 0 || patch < 0) {
      throw new IllegalArgumentException("Negative version numbers are not allowed!");
    }
    this.major = major;
    this.minor = minor;
    this.patch = patch;
  }

  /**
   * Parse a string representation (e.g., "1.2.3") into a SemanticVersion.
   * Allows only the core (MAJOR.MINOR.PATCH) without extras.
   *
   * @throws NullPointerException     string is null
   * @throws IllegalArgumentException illegal format or int overflow.
   */
  public static SemanticVersion parse(String input) {
    Objects.requireNonNull(input, "input");
    String s = input.trim();
    Matcher m = VERSION_PATTERN.matcher(s);
    if (!m.matches()) {
      throw new IllegalArgumentException("Invalid SemVer format (MAJOR.MINOR.PATCH only, without any extras)" + input);
    }
    try {
      int major = Integer.parseInt(m.group(1));
      int minor = Integer.parseInt(m.group(2));
      int patch = Integer.parseInt(m.group(3));
      return new SemanticVersion(major, minor, patch);
    } catch (NumberFormatException nfe) {
      // There might be a number too big for int
      throw new IllegalArgumentException("At least one version number is too big: " + input, nfe);
    }
  }

  /**
   * Try to parse a string representation into a SemanticVersion.
   *
   * @param input The input string.
   * @return An Optional containing the SemanticVersion if parsing was successful, otherwise Optional.empty().
   */
  public static Optional<SemanticVersion> tryParse(String input) {
    Optional<SemanticVersion> result = Optional.empty();
    if (input != null) {
      try {
        result = Optional.of(parse(input));
      } catch (RuntimeException ex) {
        // Parsing failed, return empty optional
      }
    }
    return result;
  }
  public int getVersionComponent(INCREMENT_LEVEL level) {
    return switch (level) {
      case MAJOR -> major;
      case MINOR -> minor;
      case PATCH -> patch;
    };
  }
  public int getMajor() { return major; }
  public int getMinor() { return minor; }
  public int getPatch() { return patch; }

  @Override
  public int compareTo(@NotNull SemanticVersion other) {
    Objects.requireNonNull(other, "other");
    int c = Integer.compare(this.major, other.major);
    if (c != 0) return c;
    c = Integer.compare(this.minor, other.minor);
    if (c != 0) return c;
    return Integer.compare(this.patch, other.patch);
  }

  /** Get level of difference between this version and another version.
   * @param other The other version to compare to.
   * @return The level of difference (MAJOR, MINOR, PATCH) or null if versions are identical.
   */
  public INCREMENT_LEVEL getDifferenceLevel(SemanticVersion other) {
    Objects.requireNonNull(other, "other");
    if (this.major != other.major) return INCREMENT_LEVEL.MAJOR;
    if (this.minor != other.minor) return INCREMENT_LEVEL.MINOR;
    if (this.patch != other.patch) return INCREMENT_LEVEL.PATCH;
    return null; // versions are identical
  }
  /**
   * Check if this version is before another version.
   *
   * @param other The other version to compare to.
   * @return true if this version is before the other version.
   */
  public boolean isBefore(SemanticVersion other) { return this.compareTo(other) < 0; }
  /**
   * Check if this version is after another version.
   *
   * @param other The other version to compare to.
   * @return true if this version is after the other version.
   */
  public boolean isAfter(SemanticVersion other)  { return this.compareTo(other) > 0; }
  /**
   * Check if this version is at least another version.
   *
   * @param other The other version to compare to.
   * @return true if this version is at least the other version.
   */
  public boolean isAtLeast(SemanticVersion other){ return this.compareTo(other) >= 0; }
  /**
   * Check if this version is at most another version.
   *
   * @param other The other version to compare to.
   * @return true if this version is at most the other version.
   */
  public boolean isAtMost(SemanticVersion other) { return this.compareTo(other) <= 0; }
  /** Increment the semantic version by one level (patch, minor, or major) and reset lower levels to 0.
   * @return New SemanticVersion with incremented version based on the specified level.
   */
  public SemanticVersion increment(INCREMENT_LEVEL level) {
    return switch (level) {
      case PATCH -> incrementPatch();
      case MINOR -> incrementMinor();
      case MAJOR -> incrementMajor();
    };
  }
  /**
   * Increment semantic version by one (patch level).
   *
   * @return New SemanticVersion with incremented patch version.
   */
  public SemanticVersion incrementPatch() { return new SemanticVersion(major, minor, Math.incrementExact(patch)); }
  /**
   * Increment minor version and reset patch to 0.
   *
   * @return New SemanticVersion with incremented minor version.
   */
  public SemanticVersion incrementMinor() { return new SemanticVersion(major, Math.incrementExact(minor), 0); }
  /**
   * Increment major version and reset minor and patch to 0.
   *
   * @return New SemanticVersion with incremented major version.
   */
  public SemanticVersion incrementMajor() { return new SemanticVersion(Math.incrementExact(major), 0, 0); }

  @Override
  public boolean equals(Object anotherObject) {
    if (this == anotherObject) return true;
    if (!(anotherObject instanceof SemanticVersion anotherSemanticVersion)) return false;
    return major == anotherSemanticVersion.major && minor == anotherSemanticVersion.minor && patch == anotherSemanticVersion.patch;
  }

  @Override
  public int hashCode() {
    int result = Integer.hashCode(major);
    result = 31 * result + Integer.hashCode(minor);
    result = 31 * result + Integer.hashCode(patch);
    return result;
  }

  @Override
  public String toString() {
    return major + "." + minor + "." + patch;
  }
}
