from typing import List, TypeVar
T = TypeVar('T')

def chunk_list(items: List[T], size: int) -> List[List[T]]:
    if size < 1:
        size = 1
    return [items[i:i+size] for i in range(0, len(items), size)]
