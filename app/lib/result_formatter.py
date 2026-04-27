"""
Unified Result Formatter

This module centralizes the processing and formatting of query results from
various database backends (Neo4j, MeTTa, Mork) into a unified JSON structure
suitable for the frontend graph visualization.
"""

from typing import TypedDict, List, Dict, Any, Tuple, Union

class LabelCount(TypedDict):
    label: str
    count: int

class ProcessedCount(TypedDict):
    node_count: int
    edge_count: int
    node_count_by_label: List[LabelCount]
    edge_count_by_label: List[LabelCount]

class NodeData(TypedDict):
    data: Dict[str, Any]

class EdgeData(TypedDict):
    data: Dict[str, Any]

class UnifiedGraphOutput(TypedDict):
    nodes: List[NodeData]
    edges: List[EdgeData]


class Result_Formatter:
    """
    Formats raw database results into a standardized graph or count representation.
    """

    NAMED_TYPES = [
        'gene_name', 'transcript_name', 'protein_name',
        'pathway_name', 'term_name'
    ]

    def format_result(self, data: Any, format_type: str, graph_components: Dict[str, Any], result_type: str = 'graph') -> Union[UnifiedGraphOutput, ProcessedCount, Dict[str, Any]]:
        """
        Main entry point for formatting database results.
        
        Args:
            data: Raw results from the database.
            format_type (str): The source database type ('neo4j', 'metta', 'mork').
            graph_components (dict): Metadata about the requested graph structure.
            result_type (str): The desired output format ('graph' or 'count').
            
        Returns:
            dict: The formatted result in the unified structure.
        """
        if result_type == 'graph':
            nodes, edges = self._process_graph(data, format_type, graph_components)
            return self._format_graph_output(nodes, edges)

        elif result_type == 'count':
            return self._process_count(data, format_type, graph_components)

        return {}

    def _process_graph(self, data: Any, format_type: str, graph_components: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Routes graph processing to the appropriate database-specific handler."""
        if format_type == "neo4j":
            return self._process_neo4j_graph(data, graph_components)

        elif format_type in ["mork", "metta"]:
            from app.services.metta.metta_seralizer import metta_seralizer

            results = data
            identifiers = None
            if isinstance(data, tuple) and len(data) == 2:
                results, identifiers = data

            tuples = metta_seralizer(results[0]) if isinstance(results, list) and results else []

            if not tuples and identifiers:
                return self._process_simple_graph(identifiers)

            return self._process_metta_graph(tuples, graph_components)

        raise ValueError(f"Unknown format_type for graph processing: {format_type}")
