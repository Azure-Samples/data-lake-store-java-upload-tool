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

  private static void printAllPaths(Stream<Path> pathStream) {
    pathStream.forEach(System.out::println);
  }

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
   */
  static void getFiles(String rootFolder,
                       String filter) throws IOException {
    assert (rootFolder != null);
    assert (!rootFolder.trim().isEmpty());
    assert (filter != null);
    assert (!filter.trim().isEmpty());

    Path root = Paths.get(rootFolder);
    if (Files.exists(root)) {
      // Setup the glob path matches
      PathMatcher pathMatcher = root.getFileSystem().getPathMatcher("glob:" + filter);

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
          try {
            Files.move(path, targetPath, StandardCopyOption.REPLACE_EXISTING);
            logger.debug("Moved partially processed file {} to {}",
                path.toString(),
                targetPath.toString());
          } catch (IOException ioe) {
            logger.error("Moving partially processed file {} to {} failed with exception: {}",
                path,
                targetPath,
                ioe.getMessage());
          }
        });
      } catch (IOException ioe) {
        logger.error("Scanning the source folder {} failed with exception: {}",
            rootFolder,
            ioe.getMessage());
        throw ioe;
      }

      // Second, ignore all the processed files
      // Finally, apply the glob pattern and prepare the stream
      try (Stream<Path> paths = Files.walk(Paths.get(rootFolder))) {
        Stream<Path> ps = paths
            .filter(isFileQualityForProcessing())
            .filter(doesFileMatchGlob(pathMatcher));
        printAllPaths(ps);
      } catch (IOException ioe) {
        logger.error("Scanning the source folder {} failed with exception: {}",
            rootFolder,
            ioe.getMessage());
        throw ioe;
      }
    } else {
      logger.error("Root folder {} does not exist or is not accessible", rootFolder);
    }
  }
}
