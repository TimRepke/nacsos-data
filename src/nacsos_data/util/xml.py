from typing import Any
from xml.etree.ElementTree import Element


def xml2dict(element: Element) -> dict[str, Any]:
    base: dict[str, Any] = {}
    for child in element:
        if child.tag not in base:
            base[child.tag] = []
        base[child.tag].append(xml2dict(child))
    base |= {f'@{attr}': val for attr, val in element.attrib.items()}
    if element.text and len(element.text.strip()) > 0:
        base['_text'] = element.text.strip()
    return base
