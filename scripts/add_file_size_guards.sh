#!/bin/bash
# Add guards to prevent large files from entering the repository
set -euo pipefail

echo "========================================="
echo "ADDING FILE SIZE GUARDS"
echo "========================================="
echo ""

# Create .gitattributes with size limits
echo "Creating .gitattributes with file size tracking..."
cat > .gitattributes << 'EOF'
# Track large files to prevent accidental commits
*.sql filter=lfs diff=lfs merge=lfs -text
*.sql.gz filter=lfs diff=lfs merge=lfs -text
*.dump filter=lfs diff=lfs merge=lfs -text
*.tar filter=lfs diff=lfs merge=lfs -text
*.tar.gz filter=lfs diff=lfs merge=lfs -text
*.zip filter=lfs diff=lfs merge=lfs -text
*.7z filter=lfs diff=lfs merge=lfs -text
*.rar filter=lfs diff=lfs merge=lfs -text
*.iso filter=lfs diff=lfs merge=lfs -text
*.dmg filter=lfs diff=lfs merge=lfs -text
*.exe filter=lfs diff=lfs merge=lfs -text
*.deb filter=lfs diff=lfs merge=lfs -text
*.rpm filter=lfs diff=lfs merge=lfs -text

# Binary files that might be large
*.mp4 filter=lfs diff=lfs merge=lfs -text
*.mov filter=lfs diff=lfs merge=lfs -text
*.avi filter=lfs diff=lfs merge=lfs -text
*.mkv filter=lfs diff=lfs merge=lfs -text
*.webm filter=lfs diff=lfs merge=lfs -text
*.mp3 filter=lfs diff=lfs merge=lfs -text
*.wav filter=lfs diff=lfs merge=lfs -text
*.flac filter=lfs diff=lfs merge=lfs -text
*.psd filter=lfs diff=lfs merge=lfs -text
*.ai filter=lfs diff=lfs merge=lfs -text
*.sketch filter=lfs diff=lfs merge=lfs -text
EOF

echo "✅ Created .gitattributes"
echo ""

# Create pre-commit hook
echo "Creating pre-commit hook to check file sizes..."
mkdir -p .git/hooks

cat > .git/hooks/pre-commit << 'EOF'
#!/bin/bash
# Pre-commit hook to prevent large files (>100MB)

MAX_SIZE=104857600  # 100MB in bytes
EXIT_CODE=0

# Check staged files
while IFS= read -r -d '' file; do
    if [ -f "$file" ]; then
        size=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null || echo 0)
        if [ "$size" -gt "$MAX_SIZE" ]; then
            size_mb=$((size / 1048576))
            echo "ERROR: File '$file' is ${size_mb}MB (>100MB limit)"
            echo "  Consider using git-lfs or excluding from repository"
            EXIT_CODE=1
        fi
    fi
done < <(git diff --cached --name-only -z)

if [ $EXIT_CODE -ne 0 ]; then
    echo ""
    echo "Commit aborted due to large files."
    echo "Options:"
    echo "  1. Use git-lfs: git lfs track '<pattern>'"
    echo "  2. Add to .gitignore"
    echo "  3. Compress the file"
    echo "  4. Store elsewhere (S3, external storage)"
fi

exit $EXIT_CODE
EOF

chmod +x .git/hooks/pre-commit
echo "✅ Created pre-commit hook"
echo ""

# Update .gitignore to exclude common large files
echo "Updating .gitignore..."
if ! grep -q "# Large files and backups" .gitignore 2>/dev/null; then
cat >> .gitignore << 'EOF'

# Large files and backups (added by file size guards)
*.sql
*.sql.gz
*.dump
*.backup
*.bak
backups/
dumps/
*.tar
*.tar.gz
*.tar.bz2
*.zip
*.7z
*.rar

# Database dumps
pgdump_*.sql*
mysqldump_*.sql*
*.pgdump
*.mysqldump

# Large binaries
*.exe
*.dmg
*.iso
*.deb
*.rpm
*.msi

# Media files
*.mp4
*.mov
*.avi
*.mkv
*.webm
*.mp3
*.wav
*.flac

# Large design files  
*.psd
*.ai
*.sketch
*.fig

# Temporary large files
*.tmp
*.temp
*.cache
EOF
fi

echo "✅ Updated .gitignore"
echo ""

# Create GitHub Actions workflow for size checking (optional)
echo "Creating GitHub Actions workflow..."
mkdir -p .github/workflows

cat > .github/workflows/file-size-check.yml << 'EOF'
name: File Size Check

on:
  pull_request:
    types: [opened, synchronize]

jobs:
  check-file-sizes:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
        with:
          fetch-depth: 0
      
      - name: Check for large files
        run: |
          MAX_SIZE=104857600  # 100MB
          FAILED=0
          
          # Check all files in PR
          for file in $(git diff --name-only origin/${{ github.base_ref }}...HEAD); do
            if [ -f "$file" ]; then
              size=$(stat -c%s "$file" 2>/dev/null || echo 0)
              if [ "$size" -gt "$MAX_SIZE" ]; then
                size_mb=$((size / 1048576))
                echo "❌ File '$file' is ${size_mb}MB (>100MB limit)"
                FAILED=1
              fi
            fi
          done
          
          if [ $FAILED -eq 1 ]; then
            echo ""
            echo "Large files detected. Please use git-lfs or exclude them."
            exit 1
          fi
          
          echo "✅ No files exceed 100MB limit"
EOF

echo "✅ Created GitHub Actions workflow"
echo ""

echo "========================================="
echo "FILE SIZE GUARDS INSTALLED"
echo "========================================="
echo ""
echo "Protection layers added:"
echo "  1. .gitattributes - Tracks large file types with git-lfs"
echo "  2. .git/hooks/pre-commit - Blocks commits with >100MB files"
echo "  3. .gitignore - Excludes common large file patterns"
echo "  4. .github/workflows/file-size-check.yml - PR validation"
echo ""
echo "Note: The pre-commit hook is local only."
echo "Team members should run this script after cloning."
echo ""
echo "To bypass hook in emergency: git commit --no-verify"