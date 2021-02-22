
#pragma once

extern "C" {
#include "postgres.h"
#include "utils/builtins.h"
}

#include <filesystem>
#include <fstream>
#include <iostream>
#include <utility>

class SchemaBuilder {
private:
    static const size_t MAGIC_SIZE = 4;

public:

    static std::vector<std::filesystem::path> GetParquetFiles(const char* _dirPath) {
        const auto dirPath = std::filesystem::path(_dirPath);
        std::vector<std::filesystem::path> allParquetFiles;
        for (auto& dirEntry : std::filesystem::recursive_directory_iterator(dirPath)) {
            if (!dirEntry.is_regular_file())
                continue;

            const auto path = dirEntry.path();
            auto f = std::ifstream(path, std::ios::binary);
            if (!f.is_open()) {
                // TODO: wrap me
                elog(WARNING, "Skipping file %s. Could not open it for reading.", path.native().c_str());
                continue;
            }

            std::string magicHeader(MAGIC_SIZE, '\0');
            f.read(&magicHeader[0], MAGIC_SIZE);
            f.close();

            if (magicHeader != "PAR1") {
                elog(WARNING, "Skipping file %s. Not a parquet file.", path.native().c_str());
                continue;
            }

            allParquetFiles.push_back(path);
        }
        return allParquetFiles;
    }

    /*
    static std::string createForeignTableQuery(const std::string& tableName,
        const std::string& schemaName, const std::string& serverName,
        const std::vector<std::filesystem::path> parquetFilePaths,
        List *fields, List *options)
    {
        std::stringstream ss("CREATE FOREIGN TABLE ");
        if (schemaName != "")
            ss << schemaName << ".";

        ss << quote_identifier(tableName.c_str()) << "(";

        bool is_first = true;
        ListCell *lc;
        foreach (lc, fields)
        {
            FieldInfo * field     = (FieldInfo *)lfirst(lc);
            char *      name      = field->name;
            Oid         pg_type   = field->oid;
            const char *type_name = format_type_be(pg_type);

            if (!is_first)
                ss << ", ";

            ss << name << " " << type_name;
            is_first = false;
        }

        ss << ") SERVER " << serverName << " OPTIONS (filename '";
        is_first = true;
        for (const auto& parquetFilePath : parquetFilePaths) {
            if (!is_first)
                ss << " ";

            ss << parquetFilePath.native();
            is_first = false;
        }

        ss << "'";

        foreach (lc, options)
        {
            DefElem *def = (DefElem *)lfirst(lc);
            ss << ", " << def->defname << " '" << defGetString(def) << "'";
        }

        ss << ")";

        return ss.str();
    }
    */
};

