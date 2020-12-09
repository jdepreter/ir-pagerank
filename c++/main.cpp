#include <iostream>
#include <fstream>
#include <map>
#include <vector>

using namespace std;

int32_t total_nodes = 875713;
double start_rank = 1.0 / total_nodes;

double alpha = 0.15;
double beta = 1 - alpha;
double eps = 0.000001;

double base_rank = alpha / total_nodes;

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
        ranks[node1] = start_rank;
        ranks[node2] = start_rank;
    }
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
        if (line == "") {
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

        node_nr++;
    }
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

    for (map<int32_t, double>::iterator it = ranks.begin(); it != ranks.end(); it++)
    {
        new_ranks[it->first] += base_rank;
    }
    double error = 0;

    for (map<int32_t, double>::iterator it = ranks.begin(); it != ranks.end(); it++)
    {
        error = max(error, abs(ranks[it->first] - new_ranks[it->first]));
    }

    ranks = new_ranks;
    return error;
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
    return 1;

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
        double error = iterate(ranks, links);
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