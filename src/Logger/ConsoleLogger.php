<?php declare(strict_types=1);

namespace Glider88\AsyncRedisStreams\Logger;

readonly class ConsoleLogger implements LoggerInterface
{
    public function emergency(\Stringable|string $message, array $context = []): void
    {
        echo "emergency: $message" . PHP_EOL;
    }

    public function alert(\Stringable|string $message, array $context = []): void
    {
        echo "alert: $message" . PHP_EOL;
    }

    public function critical(\Stringable|string $message, array $context = []): void
    {
        echo "critical: $message" . PHP_EOL;
    }

    public function error(\Stringable|string $message, array $context = []): void
    {
        echo "error: $message" . PHP_EOL;
    }

    public function warning(\Stringable|string $message, array $context = []): void
    {
        echo "warning: $message" . PHP_EOL;
    }

    public function notice(\Stringable|string $message, array $context = []): void
    {
        echo "notice: $message" . PHP_EOL;
    }

    public function info(\Stringable|string $message, array $context = []): void
    {
        echo "info: $message" . PHP_EOL;
    }

    public function debug(\Stringable|string $message, array $context = []): void
    {
        echo "debug: $message" . PHP_EOL;
    }

    public function log($level, \Stringable|string $message, array $context = []): void
    {
        echo "log($level): $message" . PHP_EOL;
    }
}
