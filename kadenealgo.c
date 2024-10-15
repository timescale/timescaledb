#include <stdio.h>
#include <limits.h> // For INT_MIN

int kadane(int arr[], int n) {
    int max_current = arr[0]; // Initialize current max sum as the first element
    int max_global = arr[0];  // Initialize global max sum as the first element

    for (int i = 1; i < n; i++) {
        // Update current max (either start a new subarray or extend the existing one)
        max_current = (arr[i] > (max_current + arr[i])) ? arr[i] : (max_current + arr[i]);

        // Update global max if the current max is greater
        if (max_current > max_global) {
            max_global = max_current;
        }
    }

    return max_global;
}

int main() {
    int arr[] = {-2, 1, -3, 4, -1, 2, 1, -5, 4}; // Example array
    int n = sizeof(arr) / sizeof(arr[0]);

    int max_sum = kadane(arr, n);
    printf("The maximum sum of a contiguous subarray is: %d\n", max_sum);

    return 0;
}
