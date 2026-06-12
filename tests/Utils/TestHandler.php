<?php declare(strict_types=1);

namespace Tests\Glider88\AsyncRedisStreams\Utils;

use Closure;
use Glider88\AsyncRedisStreams\MessageHandlerInterface;

class TestHandler implements MessageHandlerInterface
{
    public array $results;

    public function __construct(
        private readonly Closure $run
    ) {}

    public function handle(array $message): void
    {
        $this->results[] = $message;
        $this->run->__invoke($message);
    }
}
