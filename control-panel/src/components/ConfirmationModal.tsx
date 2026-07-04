import { useEffect, useRef, type KeyboardEvent } from 'react'

interface ConfirmationModalProps {
  isOpen: boolean
  title: string
  message: string
  confirmLabel: string
  cancelLabel?: string
  onConfirm: () => void
  onClose: () => void
}

const focusableSelector = [
  'button:not([disabled])',
  'a[href]',
  'input:not([disabled])',
  'select:not([disabled])',
  'textarea:not([disabled])',
  '[tabindex]:not([tabindex="-1"])',
].join(',')

export function ConfirmationModal({
  isOpen,
  title,
  message,
  confirmLabel,
  cancelLabel = 'Cancel',
  onConfirm,
  onClose,
}: ConfirmationModalProps) {
  const dialogRef = useRef<HTMLDivElement>(null)
  const previousFocusRef = useRef<HTMLElement | null>(null)

  useEffect(() => {
    if (!isOpen) {
      return undefined
    }

    previousFocusRef.current = document.activeElement instanceof HTMLElement ? document.activeElement : null
    const focusableElements = dialogRef.current?.querySelectorAll<HTMLElement>(focusableSelector)
    focusableElements?.[0]?.focus()

    return () => {
      previousFocusRef.current?.focus()
      previousFocusRef.current = null
    }
  }, [isOpen])

  if (!isOpen) {
    return null
  }

  const handleKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (event.key === 'Escape') {
      event.preventDefault()
      onClose()
      return
    }

    if (event.key !== 'Tab') {
      return
    }

    const focusableElements = Array.from(
      dialogRef.current?.querySelectorAll<HTMLElement>(focusableSelector) ?? [],
    )

    if (focusableElements.length === 0) {
      event.preventDefault()
      return
    }

    const firstElement = focusableElements[0]
    const lastElement = focusableElements[focusableElements.length - 1]

    if (event.shiftKey && document.activeElement === firstElement) {
      event.preventDefault()
      lastElement.focus()
    } else if (!event.shiftKey && document.activeElement === lastElement) {
      event.preventDefault()
      firstElement.focus()
    }
  }

  return (
    <div className="modal-backdrop">
      <div
        aria-modal="true"
        className="modal"
        onKeyDown={handleKeyDown}
        ref={dialogRef}
        role="dialog"
        aria-labelledby="confirmation-modal-title"
        aria-describedby="confirmation-modal-message"
      >
        <h2 id="confirmation-modal-title">{title}</h2>
        <p id="confirmation-modal-message">{message}</p>
        <div className="modal-actions">
          <button className="button secondary" type="button" onClick={onClose}>
            {cancelLabel}
          </button>
          <button className="button danger" type="button" onClick={onConfirm}>
            {confirmLabel}
          </button>
        </div>
      </div>
    </div>
  )
}
