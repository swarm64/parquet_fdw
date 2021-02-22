#pragma once

#include <cstdarg>
#include <cstdio>
#include <exception>

struct Error : std::exception
{
    char text[1000];

    Error(char const *fmt, ...) __attribute__((format(printf, 2, 3)));

    char const *what() const noexcept override;
};
