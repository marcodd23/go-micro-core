#!/bin/bash

# Fetch the latest released tag
echo "Latest released tag: $LAST_RELEASE_TAG"
echo "New pre-release tag: $NEW_PRERELEASE_TAG"

# Fetch all releases
all_releases=$(gh api -H "Accept: application/vnd.github+json" -H "X-GitHub-Api-Version: 2022-11-28" /repos/${GITHUB_REPOSITORY}/releases)

# Extract the IDs and tags of all pre-releases greater than the latest released tag
pre_releases_list=$(echo "$all_releases" | jq --raw-output --arg latest_tag "$LAST_RELEASE_TAG" \
'map(select(.prerelease and .tag_name > $latest_tag)) | .[] | "\(.id) \(.tag_name)"')

if [[ -z "$pre_releases_list" ]]; then
  echo "No pre-releases to delete."
  exit 0
fi

echo "Pre-release IDs and tags to delete: $pre_releases_list"

# Check if any pre-release tag matches the new pre-release tag
while read -r prerelease; do
  prerelease_id=$(echo "$prerelease" | cut -d ' ' -f 1)
  prerelease_tag=$(echo "$prerelease" | cut -d ' ' -f 2)

  if [[ "$prerelease_tag" == "$NEW_PRERELEASE_TAG" ]]; then
    echo "Error: Pre-release tag '$NEW_PRERELEASE_TAG' already exists."
    exit 1
  fi
done <<< "$pre_releases_list"

# Loop through the pre-release IDs and delete each one
while read -r prerelease; do
  prerelease_id=$(echo "$prerelease" | cut -d ' ' -f 1)
  prerelease_tag=$(echo "$prerelease" | cut -d ' ' -f 2)

  echo "Deleting pre-release ID: $prerelease_id with tag: $prerelease_tag"
  gh api -X DELETE -H "Accept: application/vnd.github+json" -H "X-GitHub-Api-Version: 2022-11-28" /repos/${GITHUB_REPOSITORY}/releases/$prerelease_id
done <<< "$pre_releases_list"
