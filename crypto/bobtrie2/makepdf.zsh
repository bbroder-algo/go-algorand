#!/usr/bin/env zsh

# Loop through files matching the pattern /tmp/trie*.dot
for dot_file in /tmp/trie*.dot; do
    # Check if the file exists before running the dot command
    if [[ -e "$dot_file" ]]; then
        # Extract the filename without the path and extension
        filename="${dot_file:t:r}"

        # Run the dot command to convert .dot to .png
        dot -Tpng "$dot_file" -o "/tmp/$filename.png"

        echo "Converted $dot_file to /tmp/$filename.png"
    else
        echo "File not found: $dot_file"
    fi
done

#!/usr/bin/env zsh

# Check if ImageMagick's 'convert' command is available
if ! command -v convert &> /dev/null; then
    echo "ImageMagick is not installed. Please install it first."
    exit 1
fi
# Create an output PDF file
output_pdf="/tmp/output.pdf"

# Initialize an empty array to store PNG files
png_files=()

# Collect PNG files in the /tmp directory and store them in the array
for png_file in /tmp/*.png; do
    # Check if the file exists
    if [[ -e "$png_file" ]]; then
        png_files+=("$png_file")
    else
        echo "File not found: $png_file"
    fi
done

# Check if any PNG files were found
if [[ ${#png_files[@]} -eq 0 ]]; then
    echo "No PNG files found in the /tmp directory."
    exit 1
fi

# Use 'convert' to stack the PNG images vertically and create a multi-page PDF
convert "${png_files[@]}" "$output_pdf"

echo "Conversion completed. Output PDF: $output_pdf"
