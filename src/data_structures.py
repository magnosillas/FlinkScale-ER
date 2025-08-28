# data_structures.py

from dataclasses import dataclass, field
from typing import Set, List
import json

@dataclass
class Attribute:
    """Representa um par nome-valor de um atributo, como 'titulo' e 'O Senhor dos Anéis'."""
    name: str
    value: str

    # Permite que a classe seja convertida para dicionário (útil para JSON)
    def to_dict(self):
        return {"name": self.name, "value": self.value}

@dataclass
class EntityProfile:
    """Representa uma entidade completa, com todos os seus atributos."""
    entity_id: str
    attributes: List[Attribute] = field(default_factory=list)
    is_source: bool = True # Para sabermos de qual dataset veio
    increment_id: int = 0

    # Permite que a classe seja convertida para dicionário
    def to_dict(self):
        return {
            "entity_id": self.entity_id,
            "attributes": [attr.to_dict() for attr in self.attributes],
            "is_source": self.is_source,
            "increment_id": self.increment_id
        }

    # Converte o objeto inteiro para uma string JSON
    def to_json(self):
        return json.dumps(self.to_dict(), indent=2)