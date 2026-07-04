import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it } from 'vitest'
import { App } from '../App'

describe('App shell', () => {
  it('exposes primary navigation and routes to implemented pages', async () => {
    const user = userEvent.setup()

    render(<App />)

    const main = screen.getByRole('main')
    expect(within(main).getByRole('heading', { level: 1, name: /mooncache control panel/i })).toBeInTheDocument()
    expect(within(main).getByRole('heading', { name: /^overview$/i })).toBeInTheDocument()
    expect(within(main).getByText(/cache value is improving/i)).toBeInTheDocument()

    const primaryNav = screen.getByRole('navigation', { name: /primary/i })
    const navItems = within(primaryNav).getAllByRole('link')
    expect(navItems.map((item) => item.textContent?.trim())).toEqual([
      'Overview',
      'Cache Analytics',
      'Nodes',
      'Tenants',
      'Vendors',
      'Cache Operations',
      'Alerts',
      'Audit Log',
    ])

    await user.click(within(primaryNav).getByRole('link', { name: /^cache operations$/i }))

    expect(within(main).getByRole('heading', { name: /^cache operations$/i })).toBeInTheDocument()
    expect(within(main).getByRole('button', { name: /warm up cache/i })).toBeInTheDocument()
  })
})
