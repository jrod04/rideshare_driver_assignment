"""File of algorithms to make the program faster"""


def binary_search(lst, search_item):
    low = 0
    high = len(lst) - 1
    while low <= high:
        mid = int((low + high) / 2)
        guess = lst[mid]
        if guess == search_item:
            return mid
        if guess > search_item:
            high = mid - 1
        elif guess <= search_item:
            low = mid + 1
    return None
