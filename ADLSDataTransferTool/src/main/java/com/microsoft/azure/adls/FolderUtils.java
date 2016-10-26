package com.microsoft.azure.adls;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Utility class to manage source folders.
 *
 * @author Gandhinath Swaminathan
 */
class FolderUtils {
  private static final Logger logger = LoggerFactory.getLogger(FolderUtils.class.getName());
  private static final String INPROGRESS_EXTENSION = ".inprogress";
  private static final String COMPLETED_EXTENSION = ".completed";

  /**
   * Predicate that checks if path matches glob pattern.
   *
   * @param pathMatcher Glob Pattern Matcher
   * @return Predicate that checks if path matches the glob pattern
   */
  private static Predicate<Path> doesFileMatchGlob(PathMatcher pathMatcher) {
    return path ->
        Files.isRegularFile(path)
            && pathMatcher.matches(path);
  }

  /**
   * Check if the path qualifies for processing.
   *
   * @return Predicate that checks if path qualifies for processing.
   */
  private static Predicate<Path> isFileQualityForProcessing() {
    return path ->
        Files.isRegularFile(path)
            && !path.toString().toLowerCase().endsWith(INPROGRESS_EXTENSION)
            && !path.toString().toLowerCase().endsWith(COMPLETED_EXTENSION);
  }

  /**
   * Moves the file from source to target path using the copy options specified.
   *
   * @param sourcePath Source
   * @param targetPath Target
   * @param copyOption CopyOption used during move
   */
  static void move(Path sourcePath, Path targetPath, CopyOption copyOption) {
    try {
      Files.move(sourcePath, targetPath, copyOption);
      logger.debug("Moved file {} to {}",
          sourcePath.toString(),
          targetPath.toString());
    } catch (IOException ioe) {
      logger.error("Moving file {} to {} failed with exception: {}",
          sourcePath,
          targetPath,
          ioe.getMessage());
    }
  }

  /**
   * Check if the path is partially processed.
   *
   * @return Predicate that checks if path is partially processed
   */
  private static Predicate<Path> isFilePartial() {
    return path ->
        Files.isRegularFile(path)
            && path.toString().toLowerCase().endsWith(INPROGRESS_EXTENSION);
  }

  /**
   * Walks the root folder, identifies all the files that need
   * to be uploaded.
   * <p>
   * Be aware: This method does mutate the files to manage state.
   *
   * @param rootFolder Root of the source folder structure
   * @param filter     Regular expression to match the files
   * @return Stream of file path that qualify for processing
   */
  static Stream<Path> getFiles(String rootFolder,
                               String filter) {
    assert (rootFolder != null);
    assert (!rootFolder.trim().isEmpty());
    assert (filter != null);
    assert (!filter.trim().isEmpty());

    Stream<Path> streamOfPaths = Stream.empty();
    boolean isPreviousPartialAttemptCleanedUp = false;

    Path root = Paths.get(rootFolder);
    if (Files.exists(root)) {
      // First, move all the partially uploaded files to original state
      try (Stream<Path> paths = Files.walk(Paths.get(rootFolder))) {
        Stream<Path> ps = paths.filter(isFilePartial());
        ps.forEach(path -> {
          Path targetPath = Paths.get(
              path
                  .toString()
                  .replace(INPROGRESS_EXTENSION, ""));
          logger.debug("Attempting to move partially processed file {} to {}",
              path.toString(),
              targetPath.toString());

          move(path, targetPath, StandardCopyOption.REPLACE_EXISTING);
        });
        isPreviousPartialAttemptCleanedUp = true;
      } catch (IOException ioe) {
        logger.error("Scanning the source folder {} failed with exception: {}",
            rootFolder,
            ioe.getMessage());
      }

      // Re-scan to account for files that were mutated by the previous step
      // Second, ignore all the processed files
      // Finally, apply the glob pattern and prepare the stream
      if (isPreviousPartialAttemptCleanedUp) {
        try (Stream<Path> paths = Files.walk(Paths.get(rootFolder))) {
          PathMatcher pathMatcher = root.getFileSystem().getPathMatcher("glob:" + filter);
          streamOfPaths = paths
              .filter(isFileQualityForProcessing())
              .filter(doesFileMatchGlob(pathMatcher));
        } catch (IOException ioe) {
          logger.error("Scanning the source folder {} failed with exception: {}",
              rootFolder,
              ioe.getMessage());
        }
      } else {
        logger.error("Partial uploads could not be cleaned up. Skipping processing.");
      }
    } else {
      logger.info("Root folder {} does not exist or is not accessible", rootFolder);
    }

    return streamOfPaths;
  }
}
