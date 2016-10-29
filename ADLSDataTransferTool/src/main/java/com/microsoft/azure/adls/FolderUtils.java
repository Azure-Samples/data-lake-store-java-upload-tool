package com.microsoft.azure.adls;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class to manage source folders.
 *
 * @author Gandhinath Swaminathan
 */
class FolderUtils {
  static final String INPROGRESS_EXTENSION = ".inprogress";
  static final String COMPLETED_EXTENSION = ".completed";
  private static final Logger logger = LoggerFactory.getLogger(FolderUtils.class.getName());

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
   * Check if the path is partially staged.
   *
   * @param extension Extension indicating the stage of partiality
   * @return Predicate that checks if path is partially processed
   */
  private static Predicate<Path> isFilePartiallyStaged(String extension) {
    return path ->
        Files.isRegularFile(path)
            && path.toString().toLowerCase().endsWith(extension);
  }

  /**
   * Cleans up partially staged files.
   *
   * @param rootFolder Root of the source folder structure
   * @param extension  Extension that needs to be cleaned up
   * @return True if the partially processed files are cleaned up
   */
  static boolean cleanUpPartitiallyStagedFiles(String rootFolder,
                                               String extension) {
    boolean isPartiallyStagedFilesCleanedup = false;

    if (Files.exists(Paths.get(rootFolder))) {
      // First, move all the partially uploaded files to original state
      try (Stream<Path> paths = Files.walk(Paths.get(rootFolder))) {
        Stream<Path> ps = paths.filter(isFilePartiallyStaged(extension));
        ps.forEach(path -> {
          Path targetPath = Paths.get(
              path
                  .toString()
                  .replace(extension, ""));
          logger.debug("Attempting to move partially staged file {} to {}",
              path.toString(),
              targetPath.toString());

          move(path, targetPath, StandardCopyOption.REPLACE_EXISTING);
        });
        ps.close();
        isPartiallyStagedFilesCleanedup = true;
      } catch (IOException ioe) {
        logger.error("Scanning the source folder {} failed with exception: {}",
            rootFolder,
            ioe.getMessage());
      }
    } else {
      logger.info("Root folder {} does not exist or is not accessible", rootFolder);
    }

    return isPartiallyStagedFilesCleanedup;
  }

  /**
   * Walks the root folder, identifies all the files that need
   * to be uploaded.
   *
   * @param rootFolder Root of the source folder structure
   * @param filter     Regular expression to match the files
   * @return List of file path that qualify for processing
   */
  static List<Path> getFiles(String rootFolder,
                             String filter) {
    assert (rootFolder != null);
    assert (!rootFolder.trim().isEmpty());
    assert (filter != null);
    assert (!filter.trim().isEmpty());

    List<Path> pathList = new ArrayList<>();
    Path root = Paths.get(rootFolder);

    if (Files.exists(root)) {
      // Re-scan to account for files that were mutated by the previous step
      // Second, ignore all the processed files
      // Finally, apply the glob pattern and prepare the stream
      try (Stream<Path> paths = Files.walk(Paths.get(rootFolder))) {
        PathMatcher pathMatcher = root.getFileSystem().getPathMatcher("glob:" + filter);
        pathList = paths
            .filter(isFileQualityForProcessing())
            .filter(doesFileMatchGlob(pathMatcher))
            .collect(Collectors.toList());
      } catch (IOException ioe) {
        logger.error("Scanning the source folder {} failed with exception: {}",
            rootFolder,
            ioe.getMessage());
      }
    } else {
      logger.info("Root folder {} does not exist or is not accessible", rootFolder);
    }

    return pathList;
  }
}
