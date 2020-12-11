#include <iostream>
#include <fstream>
#include <map>
#include <vector>

using namespace std;

int32_t total_nodes;
double start_rank;
double alpha = 0.15;
double beta = 1 - alpha;
double eps = 0.000001;

double base_rank;

void readfile(string filepath, std::map<int32_t, std::vector<int32_t>> &links, std::map<int32_t, double> &ranks)
{
    ifstream myfile;
    myfile.open(filepath);

    std::string line;
    std::string delimiter = "\t";
    size_t pos = 0;

    std::string token;
    std::string token2;

    while (std::getline(myfile, line))
    {
        if (line[0] == '#')
            continue;
        pos = line.find(delimiter);
        token = line.substr(0, pos);
        token2 = line.substr(pos + delimiter.length(), line.length());

        int32_t node1 = std::stoi(token);
        int32_t node2 = std::stoi(token2);

        links[node1].push_back(node2);
        ranks[node1] = 0;
        ranks[node2] = 0;
        // vector<int32_t>(links[node1]).swap(links[node1]);   // saves about 8 MB
    }
    // cout << mapCapacity(links) << endl;
    total_nodes = ranks.size();
    start_rank = 1.0 / total_nodes;
    base_rank = alpha / total_nodes;

    for (map<int32_t, double>::iterator it = ranks.begin(); it != ranks.end(); it++)
    {
        it->second = start_rank;
    }

    // cout << "Total nodes: " << total_nodes << endl;
    myfile.close();
}

void readBigFile(string filepath, std::map<int32_t, std::vector<int>> &links, std::map<int32_t, double> &ranks)
{
    ifstream myfile;
    myfile.open(filepath);

    std::string line;
    std::string delimiter = " ";
    size_t pos = 0;

    std::string start_node;
    std::string token;

    int32_t node_nr = -1;

    while (std::getline(myfile, line))
    {
        if (node_nr == -1)
        {
            node_nr++;
            continue;
        }
        if (line == "")
        {
            node_nr++;
            continue;
        }

        ranks[node_nr] = start_rank;

        while ((pos = line.find(delimiter)) != std::string::npos)
        {
            token = line.substr(0, pos);
            // cout << token << endl;
            int32_t node2 = std::stoi(token);

            ranks[node2] = start_rank;
            links[node_nr].push_back(node2);

            line.erase(0, pos + delimiter.length());
        }
        token = line;
        int32_t node2 = std::stoi(token);
        ranks[node2] = start_rank;
        links[node_nr].push_back(node2);
        vector<int32_t>(links[node_nr]).swap(links[node_nr]); // save some memory
        node_nr++;
    }
    total_nodes = ranks.size();
    myfile.close();
}

double iterate(std::map<int32_t, double> &ranks, std::map<int32_t, std::vector<int32_t>> &links)
{

    map<int32_t, double> new_ranks;
    for (map<int32_t, vector<int>>::iterator it = links.begin(); it != links.end(); it++)
    {
        // std::vector<int> out = it->second;

        for (std::vector<int>::iterator v_it = it->second.begin(); v_it != it->second.end(); ++v_it)
        {
            new_ranks[*v_it] += beta * ranks[it->first] / it->second.size();
        }
    }

    double error = 0;
    for (map<int32_t, double>::iterator it = ranks.begin(); it != ranks.end(); it++)
    {
        new_ranks[it->first] += base_rank;
        error = max(error, abs(ranks[it->first] - new_ranks[it->first]));
    }

    ranks = new_ranks;
    return error;
}

double random_walk(std::map<int32_t, double> &ranks, std::map<int32_t, std::vector<int32_t>> &links, int current_iteration)
{

    // TODO: set base rank for nodes that are not visited
    int iterations_per = 1;
    int total_visits = 0;
    map<int32_t, double> visits;

    for (int i = 0; i < iterations_per; i++)
    {
        for (map<int32_t, vector<int>>::iterator it = links.begin(); it != links.end(); it++)
        {
            int32_t current_node = it->first;
            bool first = true;
            while (true)
            {
                if (!first)
                {
                    total_visits += 1;
                    visits[current_node] += 1;
                }
                first = false;

                // random double between [0.0, 1.0[
                double r = static_cast<double>(rand()-1) / static_cast<double>(RAND_MAX);
                if (r < alpha)
                {
                    break;
                }
                else
                {
                    // Jump to random node in list
                    if (links[current_node].size() > 0)
                    {
                        r = (r - alpha) * 1.0 / (1 - alpha); // Redistribute 0.15 - 1.0 -> 0.0 - 1.0
                        r = r * links[current_node].size();
                        int choise = (int)r;
                        current_node = links[current_node][choise];
                    }
                    else
                    {
                        break;
                    }
                }
            }
        }
    }

    total_visits += iterations_per * ranks.size();
    double error = 0;
    for (map<int32_t, double>::iterator it = ranks.begin(); it != ranks.end(); it++)
    {
        // Compute distribution
        visits[it->first] = (iterations_per + visits[it->first]) / (double)total_visits;

        if (current_iteration > 0) {
            visits[it->first] = (ranks[it->first] * current_iteration + visits[it->first]) / (double)(current_iteration + 1);
        }
        

        error = max(error, abs(visits[it->first] - ranks[it->first]));
    }
    ranks = visits;

    return error;

    /*

    total_visits += ranks.size(); // We skipped the random walk in the previous for loop
    for (map<int32_t, double>::iterator it = visits.begin(); it != visits.end(); it++)
    {
        // Compute distribution
        visits[it->first] /= (double)total_visits;
    }
    for (map<int32_t, double>::iterator it = ranks.begin(); it != ranks.end(); it++)
    {
        // Update the rank
        // if current iteration is 2, we have had 2 iterations: 0 and 1
        // we weigh the old rank in regard to the amount of past iterations
        // (old_rank * nr_iteration) + new_rank ) / (nr_iteration+1)
        ranks[it->first] = (ranks[it->first] * current_iteration + (visits[it->first] + base_rank)) / (double)(current_iteration + 1);
    }
    */
    return 0; // TODO CHANGE
}

int32_t main(int argc, char *argv[])
{
    string filepath = "../data/web-Google.txt";
    string filepath_out = "out.csv";
    string format = "google";

    // cout << argc << endl;

    // Read cli args
    switch (argc)
    {
    case 2:
        cerr << "Missing format (options: 'google', 'graph-txt')" << endl;
        cerr << "Example: ./a.out " << argv[1] << " google" << endl;
        return 1;
        break;

    case 3:
        filepath = argv[1];
        format = argv[2];
        cout << "File: " << filepath << endl;
        cout << "Format: " << format << endl;
        cout << "File Out: " << filepath_out << endl;
        break;

    case 4:
        filepath = argv[1];
        format = argv[2];
        filepath_out = argv[3];
        cout << "File: " << filepath << endl;
        cout << "Format: " << format << endl;
        cout << "File Out: " << filepath_out << endl;
        break;

    default:
        break;
    }

    // Initialize datastructures
    std::map<int32_t, vector<int32_t>> links;
    std::map<int32_t, double> ranks;

    // Read file
    if (format == "google")
    {
        readfile(filepath, links, ranks);
    }
    else if (format == "graph-txt")
    {
        readBigFile(filepath, links, ranks);
    }
    else
    {
        cerr << "Unsupported file format: " << format << endl;
        return 1;
    }

    cout << "Done Reading" << endl;
    cout << "Total nodes: " << total_nodes << endl;

    // return 1;

    // for (size_t i = 0; i < links[0].size(); i++)
    // {
    //     cout << links[0][i] << " ";
    // }
    // cout << endl;

    // cout << ranks.size() << endl;

    // Start iterating
    int32_t i = 0;
    while (true)
    {
        std::cout << "Start Iteration " << i << endl;
        double error = random_walk(ranks, links, i);
        std::cout << "End Iteration " << i << " with error " << error << endl;
        if (error < eps)
        {
            std::cout << "Convergence achieved" << endl;
            break;
        }
        i++;
    }

    // cout << ranks.size() << endl;

    // Write to csv file format
    cout << "Writing file" << endl;
    ofstream out;
    out.open(filepath_out);
    for (map<int32_t, double>::iterator it = ranks.begin(); it != ranks.end(); it++)
    {
        out << it->first << ";" << it->second << endl;
    }
    out.close();

    return 0;
}