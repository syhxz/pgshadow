// Package capture provides network packet capture implementations for
// PostgreSQL traffic capture. This file contains utility functions for
// BPF filter validation.
package capture

import (
	"fmt"
	"os/exec"
	"strings"
)

// ValidateBPFFilter checks if a BPF filter is syntactically valid by attempting
// to compile it using tcpdump. Returns nil if valid, or an error describing
// the problem if invalid.
func ValidateBPFFilter(filter string) error {
	if strings.TrimSpace(filter) == "" {
		return fmt.Errorf("BPF filter is empty")
	}

	// Use tcpdump to validate the filter syntax
	// The -ddd flag outputs the filter in numeric format, which requires
	// successful compilation
	// We use -r /dev/null to avoid needing an actual interface
	cmd := exec.Command("tcpdump", "-ddd", "-i", "lo", filter)
	_, err := cmd.Output()
	
	if err != nil {
		// Try alternative validation with -s 0 (snaplen) and just check exit code
		cmd := exec.Command("tcpdump", "-s", "0", "-c", "0", "-i", "lo", filter)
		_, err2 := cmd.CombinedOutput()
		
		if err2 != nil {
			// Extract a meaningful error message
			errMsg := "invalid BPF filter syntax"
			if ee, ok := err.(*exec.ExitError); ok {
				errMsg = strings.TrimSpace(string(ee.Stderr))
				if errMsg == "" {
					errMsg = ee.Error()
				}
			}
			return fmt.Errorf("BPF filter validation failed: %s", errMsg)
		}
	}
	
	return nil
}

// ValidateBPFFilterWithTimeout validates a BPF filter with a timeout to prevent
// hanging on malformed filters. The timeout is in seconds.
func ValidateBPFFilterWithTimeout(filter string, timeoutSeconds int) error {
	if strings.TrimSpace(filter) == "" {
		return fmt.Errorf("BPF filter is empty")
	}

	// Use timeout command if available
	cmd := exec.Command("timeout", fmt.Sprintf("%d", timeoutSeconds), 
		"tcpdump", "-ddd", "-i", "lo", filter)
	_, err := cmd.Output()
	
	if err != nil {
		// Try with combined output for better error messages
		cmd := exec.Command("timeout", fmt.Sprintf("%d", timeoutSeconds),
			"tcpdump", "-s", "0", "-c", "0", "-i", "lo", filter)
		_, err2 := cmd.CombinedOutput()
		
		if err2 != nil {
			errMsg := "invalid BPF filter syntax"
			if ee, ok := err.(*exec.ExitError); ok {
				errMsg = strings.TrimSpace(string(ee.Stderr))
				if errMsg == "" {
					errMsg = ee.Error()
				}
			} else if err2 != nil {
				if ee, ok := err2.(*exec.ExitError); ok {
					errMsg = strings.TrimSpace(string(ee.Stderr))
					if errMsg == "" {
						errMsg = ee.Error()
					}
				}
			}
			return fmt.Errorf("BPF filter validation failed: %s", errMsg)
		}
	}
	
	return nil
}

// IsBPFToolAvailable checks if tcpdump is available for BPF filter validation.
func IsBPFToolAvailable() bool {
	cmd := exec.Command("tcpdump", "--version")
	err := cmd.Run()
	return err == nil
}

// QuickBPFValidation provides a lightweight validation without spawning
// external processes. It checks for common issues in BPF filter strings.
func QuickBPFFilterValidation(filter string) error {
	if strings.TrimSpace(filter) == "" {
		return fmt.Errorf("BPF filter is empty")
	}

	// Check for unbalanced parentheses
	openCount := strings.Count(filter, "(")
	closeCount := strings.Count(filter, ")")
	if openCount != closeCount {
		return fmt.Errorf("BPF filter has unbalanced parentheses: %d '(' vs %d ')'", 
			openCount, closeCount)
	}

	// Check for unbalanced quotes
	singleQuotes := strings.Count(filter, "'")
	if singleQuotes%2 != 0 {
		return fmt.Errorf("BPF filter has unbalanced single quotes")
	}

	// Check for empty parentheses (common syntax error)
	if strings.Contains(filter, "()") {
		return fmt.Errorf("BPF filter contains empty parentheses")
	}

	// Basic length check
	if len(filter) > 4096 {
		return fmt.Errorf("BPF filter is too long (max 4096 characters)")
	}

	return nil
}
