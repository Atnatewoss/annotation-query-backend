#include "graph_core.hpp"
#include <random>
#include <algorithm>
#include <sstream>
#include <set>
#include <tuple>

using namespace std;


string generate_nanoid() {
    static const char alphabet[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static random_device rd;
    static mt19937 gen(rd());
    static uniform_int_distribution<> dis(0, sizeof(alphabet) - 2);
    
    string id = "";
    for (int i = 0; i < 21; ++i) {
        id += alphabet[dis(gen)];
    }
    return id;
}

string extract_middle(const string& s) {
    size_t first = s.find('_');
    size_t last = s.rfind('_');
    if (first == string::npos || first == last) {
        if (first != string::npos) return s.substr(first + 1);
        return "";
    }
    return s.substr(first + 1, last - first - 1);
}

Graph group_node_only(const Graph& graph, const py::dict& request) {
    Graph new_graph;
    unordered_map<string, vector<py::dict>> node_map_by_label;
    
    py::list request_nodes = request["nodes"];
    for (auto item : request_nodes) {
        py::dict node = item.cast<py::dict>();
        node_map_by_label[node["type"].cast<string>()] = {};
    }
    
    for (const auto& node : graph.nodes) {
        string type = node.attrs.at("type").cast<string>();
        if (node_map_by_label.count(type)) {
            py::dict data;
            for (auto const& [key, val] : node.attrs) {
                data[py::str(key)] = val;
            }
            node_map_by_label[type].push_back(data);
        }
    }
    
    for (auto& [node_type, nodes] : node_map_by_label) {
        if (nodes.empty()) continue;
        
        Node new_node;
        new_node.id = generate_nanoid();
        new_node.attrs["id"] = py::cast(new_node.id);
        new_node.attrs["type"] = py::cast(node_type);
        new_node.attrs["name"] = py::cast(to_string(nodes.size()) + " " + node_type + " nodes");
        
        py::list nodes_list;
        for (const auto& n : nodes) nodes_list.append(n);
        new_node.attrs["nodes"] = nodes_list;
        
        new_graph.nodes.push_back(new_node);
    }
    
    return new_graph;
}

pair<unordered_map<string, unordered_map<string, py::dict>>, unordered_map<string, py::dict>> get_node_to_connections_map(const Graph& graph) {
    unordered_map<string, py::dict> node_to_id_map;
    for (const auto& node : graph.nodes) {
        py::dict data;
        for (auto const& [key, val] : node.attrs) data[py::str(key)] = val;
        node_to_id_map[node.id] = data;
    }
    
    unordered_map<string, unordered_map<string, py::dict>> node_mapping;
    
    auto add_to_map = [&](const Edge& edge, bool is_source) {
        string node_key = is_source ? edge.source : edge.target;
        string other_node = is_source ? edge.target : edge.source;
        
        auto& connections = node_mapping[node_key];
        if (connections.find(edge.edge_id) == connections.end()) {
            py::dict conn;
            conn["is_source"] = py::cast(is_source);
            conn["nodes"] = py::set();
            conn["edge_id"] = py::cast(edge.edge_id);
            connections[edge.edge_id] = conn;
        }
        connections[edge.edge_id]["nodes"].cast<py::set>().add(other_node);
    };
    
    for (const auto& edge : graph.edges) {
        add_to_map(edge, true);
        add_to_map(edge, false);
    }
    
    return {node_mapping, node_to_id_map};
}
