#include "Error.hpp"

Error::Error(char const *fmt, ...)
{
    std::va_list ap;
    va_start(ap, fmt);
    std::vsnprintf(text, sizeof(text), fmt, ap);
    va_end(ap);
}

char const *Error::what() const noexcept
{
    return text;
}
