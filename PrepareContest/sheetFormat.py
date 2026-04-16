import gspread

# Create a list of cell ranges and their corresponding formats
def alternate_colors(sheet, start_row, end_row, column_range):
    cols=column_range.split(":")
    cell_formats = []
    for row in range(start_row, end_row + 1):
        cell_range = f"{cols[0]}{row}{cols[1]}{row}"
        if (row - start_row) % 2 == 0:
            cell_formats.append({
                "range": cell_range,
                "format": {
                    "backgroundColor": {
                        "red": 0.9,
                        "green": 0.9,
                        "blue": 0.9
                    }
                }
            })
        else:
            cell_formats.append({
                "range": cell_range,
                "format": {
                    "backgroundColor": {
                        "red": 0.95,
                        "green": 0.95,
                        "blue": 0.95
                    }
                }
            })

    # Apply the cell formats using the batch_format method
    sheet.batch_format(cell_formats)

def header_colors(sheet, cell_range):
    # Color the background  range in black,
    # change horizontal alignment, text color and font size
    sheet.format(cell_range, {
        "backgroundColor": {
        "red": 0.0,
        "green": 0.0,
        "blue": 0.0
        },
        "horizontalAlignment": "CENTER",
        "textFormat": {
        "foregroundColor": {
            "red": 1.0,
            "green": 1.0,
            "blue": 1.0
        },
        "fontSize": 12,
        "bold": True
        }
    })