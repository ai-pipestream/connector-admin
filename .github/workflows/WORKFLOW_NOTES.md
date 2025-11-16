# Workflow Configuration Notes

## Release and Publish Workflow

This workflow is based on the validated configuration from `platform-registration-service` with the following adaptations:

### Similarities with platform-registration-service
- Manual version calculation approach (parsing current version and incrementing)
- Direct git tag creation instead of axion-release tasks
- Same job structure: build → release → docker-production
- Same approach for GPG signing configuration
- Similar Docker image build and push process

### Intentional Differences

1. **Input Parameter Names**:
   - connector-admin uses: `version_bump` and `manual_version`
   - platform-registration-service uses: `release_type` and `custom_version`
   - **Rationale**: Both naming conventions are clear and self-documenting. No change needed.

2. **Build Command Flags**:
   - connector-admin: `./gradlew clean build test --no-daemon`
   - platform-registration-service: `./gradlew clean build test --no-daemon --refresh-dependencies`
   - **Rationale**: The `--refresh-dependencies` flag is not strictly necessary for most builds and can slow down the build process. connector-admin omits it for better performance.

3. **Publishing Commands**:
   - connector-admin uses: `publishAllPublicationsToGitHubPackagesRepository`
   - platform-registration-service uses: `publish -PskipCentral=true`
   - **Rationale**: Different project setups. connector-admin's approach is more explicit about the destination.

4. **Release Assets**:
   - connector-admin copies specific JAR files from `build/libs/` and `build/quarkus-app/`
   - platform-registration-service creates a zip file with multiple artifacts
   - **Rationale**: Different artifact structures based on project needs.

### Version Calculation Fix

The key fix implemented was replacing the unreliable axion-release dry-run approach:

**Before (Broken)**:
```bash
VERSION=$(./gradlew release -Prelease.versionIncrementer=incrementMajor -Prelease.dryRun | grep "Release version" | awk '{print $NF}')
```

**After (Fixed)**:
```bash
CURRENT=$(./gradlew currentVersion -q --no-daemon | tail -1)
CURRENT=${CURRENT#Project version: }
CURRENT=${CURRENT%-SNAPSHOT}
CURRENT=$(echo "$CURRENT" | sed 's/-[a-zA-Z0-9-]*$//')
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT"
VERSION="${MAJOR}.${MINOR}.$((PATCH + 1))"  # for patch increment
```

This approach:
- Gets the current version reliably
- Handles branch suffixes in version strings
- Manually increments version components
- Ensures proper output format for downstream jobs

## Validation

The workflow should be tested by:
1. Running a test release with `patch` version bump
2. Verifying the version is correctly calculated and tag is created
3. Confirming the Docker image is built with the correct version tag
4. Checking that GitHub release is created with proper artifacts
