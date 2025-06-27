#include <oneapi/tbb/flow_graph.h>
#include <iostream>
#include <tuple>

namespace flow = oneapi::tbb::flow;

int square(int x) { return x * x; }
int cube(int x) { return x * x * x; }
int sum(int x, int y) { return x + y; }

int main()
{
    flow::graph g;
    flow::broadcast_node<int> input(g);
    flow::function_node<int, int> squarer(g, flow::unlimited, square);
    flow::function_node<int, int> cuber(g, flow::unlimited, cube);
    flow::join_node<std::tuple<int, int>, flow::queueing> join(g);
    flow::function_node<std::tuple<int, int>, int> summer(g, flow::serial, [](const std::tuple<int, int>& pair) {
        return sum(std::get<0>(pair), std::get<1>(pair));
    });

    flow::make_edge(input, squarer);
    flow::make_edge(input, cuber);
    flow::make_edge(squarer, std::get<0>(join.input_ports()));
    flow::make_edge(cuber, std::get<1>(join.input_ports()));
    flow::make_edge(join, summer);

    for (int i = 1; i <= 3; ++i) {
        input.try_put(i);
    }

    g.wait_for_all();

    return 0;
}
