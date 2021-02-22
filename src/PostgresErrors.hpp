#pragma once

#ifdef __clang__
#elif __GNUC__
#pragma GCC diagnostic ignored "-Wunused-but-set-variable"
#endif

extern "C" {
#include "postgres.h"
}

#include <exception>
#include <memory>
#include <stdexcept>
#include <type_traits>

struct PostgresErrorDeleter
{
    void operator()(ErrorData *eData) const noexcept
    {
        FreeErrorData(eData);
    }
};

using PostgresErrorPtr = std::unique_ptr<ErrorData, PostgresErrorDeleter>;

struct PostgresError : public std::exception
{
    explicit PostgresError(PostgresErrorPtr error) : Error(std::move(error))
    {
    }

    PostgresErrorPtr Error;
};

template <typename Function>
auto CatchAndRethrow(Function fun)
{
    using ReturnType                 = std::invoke_result_t<Function>;
    constexpr auto shouldReturnValue = !std::is_same_v<ReturnType, void>;
    std::conditional_t<shouldReturnValue, ReturnType, int> result{};

    auto context              = CurrentMemoryContext;
    ErrorData *volatile eData = nullptr;
    auto *volatile resultPtr  = &result;

    PG_TRY();
    {
        if constexpr (shouldReturnValue)
            *resultPtr = fun();
        else
            fun();
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(context);
        eData = CopyErrorData();
        FlushErrorState();
    }
    PG_END_TRY();

    if (eData != nullptr)
    {
        PostgresErrorPtr error(eData);
        throw std::runtime_error(error->message);
    }

    if constexpr (shouldReturnValue)
        return result;
}
