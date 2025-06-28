def load_txt(filename: str) -> list:
    """
    Loads a txt file and returns its content as a list of strings.

    Args:
        filename (str): The path to the txt file.

    Returns:
        list: A list with each line of the txt file as an element.

    Example:
        load_text("video_urls.txt")
    """
    with open(filename, mode="r", newline="") as file:
        data = file.readlines()
    data = [line.strip() for line in data if line.strip()]  # Remove empty
    return data