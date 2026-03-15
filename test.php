<?php
declare(strict_types=1);

/**
 * Return the sum of two numbers.
 *
 * @param float|int $a
 * @param float|int $b
 * @return float
 */
function sumNumbers(float|int $a, float|int $b): float {
    return $a + $b;
}

// Example usage:
// - CLI: php test.php 2 3
// - HTTP: /test.php?a=2&b=3
if (php_sapi_name() === 'cli') {
    $a = isset($argv[1]) ? (float)$argv[1] : 0.0;
    $b = isset($argv[2]) ? (float)$argv[2] : 0.0;
    echo sumNumbers($a, $b) . PHP_EOL;
} else {
    $a = isset($_GET['a']) ? (float)$_GET['a'] : 0.0;
    $b = isset($_GET['b']) ? (float)$_GET['b'] : 0.0;
    echo sumNumbers($a, $b);
}