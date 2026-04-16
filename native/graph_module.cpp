#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include "graph_core.hpp"

using namespace std;


namespace py = pybind11;

Graph py_to_graph(const py::dict& obj) {
    Graph g;
    if (obj.contains("nodes")) {
        py::list nodes = obj["nodes"].cast<py::list>();
        for (auto item : nodes) {
            py::dict n_dict = item.cast<py::dict>();
            Node n;
            py::dict data = n_dict.contains("data") ? n_dict["data"].cast<py::dict>() : n_dict;
            n.id = data["id"].cast<string>();
            for (auto const& [k, v] : data) {
                n.attrs[k.cast<string>()] = v.cast<py::object>();
            }
            g.nodes.push_back(n);
        }
    }
    if (obj.contains("edges")) {
        py::list edges = obj["edges"].cast<py::list>();
        for (auto item : edges) {
            py::dict e_dict = item.cast<py::dict>();
            Edge e;
            py::dict data = e_dict.contains("data") ? e_dict["data"].cast<py::dict>() : e_dict;
            e.id = data.contains("id") ? data["id"].cast<string>() : generate_nanoid();
            e.source = data["source"].cast<string>();
            e.target = data["target"].cast<string>();
            e.edge_id = data.contains("edge_id") ? data["edge_id"].cast<string>() : "";
            e.label = data.contains("label") ? data["label"].cast<string>() : "";
            for (auto const& [k, v] : data) {
                e.attrs[k.cast<string>()] = v.cast<py::object>();
            }
            g.edges.push_back(e);
        }
    }
    return g;
}

