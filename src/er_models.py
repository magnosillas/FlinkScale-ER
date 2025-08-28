from dataclasses import dataclass, field
from typing import Set, List, Tuple
import re

# Representa um único atributo de uma entidade (ex: "nome", "Ford")
@dataclass(frozen=True)
class Attribute:
    name: str
    value: str

# Representa uma entidade completa (ex: um produto, uma pessoa)
@dataclass
class EntityProfile:
    entity_id: str
    attributes: Set[Attribute]
    source_name: str # Para saber se vem da Fonte 1 ou Fonte 2
    
    # Este método recria a lógica de tokenização do Java
    def get_tokens(self) -> Set[str]:
        """Extrai tokens de todos os atributos da entidade."""
        all_tokens = set()
        for attr in self.attributes:
            # Lógica simples de tokenização: separar por palavras e deixar em minúsculo
            # Remove caracteres não alfanuméricos
            clean_value = re.sub(r'\W+', ' ', attr.value).lower()
            tokens = set(clean_value.split())
            all_tokens.update(tokens)
        return all_tokens

# Representa um "nó" no grafo de similaridade, contendo a entidade e seus vizinhos
@dataclass
class EntityNode:
    profile: EntityProfile
    # Armazena os vizinhos e suas similaridades: (id_vizinho, similaridade)
    neighbors: List[Tuple[str, float]] = field(default_factory=list)

# Representa um bloco de entidades que compartilham o mesmo token
@dataclass
class Block:
    blocking_key: str
    source_1_entities: List[EntityNode] = field(default_factory=list)
    source_2_entities: List[EntityNode] = field(default_factory=list)