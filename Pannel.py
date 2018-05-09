from PIL import Image
import StreamDeck.StreamDeck as StreamDeck



def _convert_png_pixels(path):
    """This function converts the png file provided into a list with the image values to transfer to a button"""
    im = Image.open(path).convert('RGB')
    px = im.load()

    pixels = []

    for x in range(72):
        for y in range(72):
            for ch in [2, 1, 0]:  # This represents the color order (BRG for the StreamDeck)
                pixels.append(px[x, y][ch])

    return pixels


def key_callback(deck, key, state):
    """A decorator function serving as a template for key changes callbacks"""
    print("Deck {} Key {} = {}".format(deck.id(), key, state), flush=True)

    if state:
        deck.set_key_image(key, get_random_key_colour_image(deck))

        if key == d.key_count() - 1:
            deck.reset()
            deck.close()



if __name__ == '__main__':
    print(_convert_png_pixels('/home/julio/Downloads/streamdeck_key.png'))
