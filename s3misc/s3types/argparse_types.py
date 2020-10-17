from argparse import ArgumentTypeError

# Custom type for argparse. I want just a single character.
def ArgParseChar(value):
    if (len(value) != 1):
        raise ArgumentTypeError("The given value is not exactly one character")
