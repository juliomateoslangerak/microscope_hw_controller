from PIL import Image


def _convert_png_pixels(path):
    """This function converts the png file provided into a list with the image values to transfer to a button"""
    im = Image.open(path).convert('RGB')
    px = im.load()

    pixels = []

    for x in range(72):
        for y in range(72):
            for ch in [2, 0, 1]:  # This represents the color order (BRG for the StreamDeck)
                pixels.append(px[x, y][ch])

    return pixels


if __name__ == '__main__':
    print(_convert_png_pixels('/home/julio/Downloads/streamdeck_key.png'))
