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
