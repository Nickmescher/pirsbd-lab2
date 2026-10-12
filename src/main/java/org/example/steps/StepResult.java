package org.example.steps;

// Результат шага: готовая markdown-секция + число строк для сводной таблицы.
public record StepResult(int number, String title, long rowsAffected, String markdown) {
}
